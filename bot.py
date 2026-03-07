import os
import random
import logging
import asyncio
from datetime import datetime, timezone

import asyncpg
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.error import Forbidden, TimedOut
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ================= CONFIG =================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN    = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID     = 643086953

VIP_REFERRAL_THRESHOLD = 3
VIP_REFERRAL_DAYS      = 3
MATCH_INVITE_DELAY     = 45

db_pool: asyncpg.Pool = None

# ================= SCHEMA =================

async def init_db():
    # All timestamps use WITH TIME ZONE — always UTC, no offset surprises
    await db_pool.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id        BIGINT PRIMARY KEY,
            username       TEXT,
            name           TEXT,
            gender         TEXT,
            country        TEXT,
            age            INT,
            is_vip         BOOLEAN DEFAULT FALSE,
            vip_expiry     TIMESTAMP WITH TIME ZONE,
            referral_count INT DEFAULT 0,
            referred_by    BIGINT,
            total_messages INT DEFAULT 0,
            is_banned      BOOLEAN DEFAULT FALSE,
            created_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    """)
    await db_pool.execute("""
        CREATE TABLE IF NOT EXISTS waiting_users (
            user_id          BIGINT PRIMARY KEY,
            preferred_gender TEXT,
            queued_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    """)
    await db_pool.execute("""
        CREATE TABLE IF NOT EXISTS active_chats (
            user_id    BIGINT PRIMARY KEY,
            partner_id BIGINT
        )
    """)
    await db_pool.execute("""
        CREATE TABLE IF NOT EXISTS reports (
            id          SERIAL PRIMARY KEY,
            reporter_id BIGINT NOT NULL,
            reported_id BIGINT NOT NULL,
            created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    """)
    await db_pool.execute(
        "CREATE INDEX IF NOT EXISTS idx_waiting_queue ON waiting_users (queued_at)"
    )
    await db_pool.execute(
        "CREATE INDEX IF NOT EXISTS idx_vip_expiry ON users (vip_expiry)"
    )
    await db_pool.execute(
        "CREATE INDEX IF NOT EXISTS idx_reports_reported ON reports (reported_id)"
    )
    await db_pool.execute(
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_banned BOOLEAN DEFAULT FALSE"
    )
    logger.info("Database initialised successfully")

# ================= KEYBOARDS =================

user_keyboard = ReplyKeyboardMarkup(
    [
        ["🚀 Find Partner"],
        ["👨 Find Male", "👩 Find Female"],
        ["⏭ Next", "❌ Stop"],
        ["💎 VIP"],
    ],
    resize_keyboard=True,
)

admin_main_keyboard = ReplyKeyboardMarkup(
    [
        ["🚀 Find Partner"],
        ["👨 Find Male", "👩 Find Female"],
        ["⏭ Next", "❌ Stop"],
        ["💎 VIP"],
        ["⚙️ Admin Panel"],
    ],
    resize_keyboard=True,
)

admin_panel_keyboard = ReplyKeyboardMarkup(
    [
        ["📊 Analytics"],
        ["📢 Announcement"],
        ["👥 Active Users", "🕒 Waiting Users"],
        ["🧹 Clean Dead Chats"],
        ["⬅ Back"],
    ],
    resize_keyboard=True,
)

vip_keyboard = ReplyKeyboardMarkup(
    [
        ["🎁 Get FREE VIP"],
        ["👑 Contact Admin"],
        ["⬅ Back"],
    ],
    resize_keyboard=True,
)

gender_keyboard = ReplyKeyboardMarkup(
    [["Male", "Female"]],
    resize_keyboard=True,
    one_time_keyboard=True,
)

def report_inline(partner_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⚠️ Report", callback_data=f"report:{partner_id}")
    ]])

# ================= HELPERS =================

def get_main_keyboard(uid: int) -> ReplyKeyboardMarkup:
    return admin_main_keyboard if uid == ADMIN_ID else user_keyboard


async def user_exists(uid: int) -> bool:
    row = await db_pool.fetchrow("SELECT 1 FROM users WHERE user_id = $1", uid)
    return row is not None


async def is_registered(uid: int) -> bool:
    row = await db_pool.fetchrow(
        "SELECT 1 FROM users WHERE user_id = $1 AND name IS NOT NULL AND gender IS NOT NULL AND age IS NOT NULL",
        uid,
    )
    return row is not None


async def is_banned(uid: int) -> bool:
    row = await db_pool.fetchrow("SELECT is_banned FROM users WHERE user_id = $1", uid)
    return bool(row and row["is_banned"])


async def get_partner(uid: int):
    row = await db_pool.fetchrow(
        "SELECT partner_id FROM active_chats WHERE user_id = $1", uid
    )
    return row["partner_id"] if row else None


async def check_vip(uid: int) -> bool:
    row = await db_pool.fetchrow(
        "SELECT is_vip, vip_expiry FROM users WHERE user_id = $1", uid
    )
    if not row:
        return False
    if row["is_vip"] and row["vip_expiry"] is None:
        return True
    if row["vip_expiry"] and row["vip_expiry"] > datetime.now(timezone.utc):
        return True
    return False


async def grant_vip(uid: int, days: int):
    await db_pool.execute(
        """
        UPDATE users
        SET is_vip     = TRUE,
            vip_expiry = GREATEST(NOW(), COALESCE(vip_expiry, NOW()))
                         + ($1 || ' days')::INTERVAL
        WHERE user_id  = $2
        """,
        days, uid,
    )


async def handle_referral(new_uid: int, referrer_uid: int) -> bool:
    if referrer_uid == new_uid:
        return False
    if not await user_exists(referrer_uid):
        return False
    row = await db_pool.fetchrow(
        """
        UPDATE users SET referral_count = referral_count + 1
        WHERE user_id = $1 RETURNING referral_count
        """,
        referrer_uid,
    )
    if row and row["referral_count"] % VIP_REFERRAL_THRESHOLD == 0:
        await grant_vip(referrer_uid, VIP_REFERRAL_DAYS)
        return True
    return False

# ================= REGISTRATION STEP DETECTOR =================

async def get_registration_step(uid: int) -> str | None:
    """Reads DB to find which step a user is on. Each field is saved immediately."""
    row = await db_pool.fetchrow(
        "SELECT name, gender, country, age FROM users WHERE user_id = $1", uid
    )
    if not row:
        return None
    if row["name"]    is None: return "name"
    if row["gender"]  is None: return "gender"
    if row["country"] is None: return "country"
    if row["age"]     is None: return "age"
    return None

# ================= CANCEL INVITE TIMER =================

def cancel_invite_timer(context: ContextTypes.DEFAULT_TYPE):
    """Cancel any running invite prompt task for this user."""
    task: asyncio.Task = context.user_data.pop("invite_task", None)
    if task and not task.done():
        task.cancel()

# ================= CLEAN DEAD CHATS =================

async def clean_dead_chats(bot) -> int:
    rows    = await db_pool.fetch("SELECT DISTINCT user_id, partner_id FROM active_chats")
    seen    = set()
    cleaned = 0

    for row in rows:
        uid, partner = row["user_id"], row["partner_id"]
        pair_key = tuple(sorted((uid, partner)))
        if pair_key in seen:
            continue
        seen.add(pair_key)

        uid_alive = partner_alive = False
        try:
            await bot.send_chat_action(chat_id=uid, action="typing")
            uid_alive = True
        except Exception:
            pass
        try:
            await bot.send_chat_action(chat_id=partner, action="typing")
            partner_alive = True
        except Exception:
            pass

        if not uid_alive or not partner_alive:
            await db_pool.execute("DELETE FROM active_chats WHERE user_id = $1", uid)
            await db_pool.execute("DELETE FROM active_chats WHERE user_id = $1", partner)
            cleaned += 1
            logger.info("Cleaned dead chat: %s <-> %s", uid, partner)
            if uid_alive:
                try:
                    await bot.send_message(uid, "⚠️ Your partner disconnected. Press 🚀 Find Partner to start again.")
                except Exception:
                    pass
            if partner_alive:
                try:
                    await bot.send_message(partner, "⚠️ Your partner disconnected. Press 🚀 Find Partner to start again.")
                except Exception:
                    pass

    return cleaned


async def cleanchats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return
    await update.message.reply_text("🔍 Scanning for dead chats...")
    cleaned = await clean_dead_chats(context.bot)
    await update.message.reply_text(f"✅ Done. Removed {cleaned} dead chat(s).")

# ================= BROADCAST =================

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    msg  = " ".join(context.args)
    rows = await db_pool.fetch("SELECT user_id FROM users")
    sent = 0
    for row in rows:
        try:
            await context.bot.send_message(row["user_id"], msg)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass
    await update.message.reply_text(f"✅ Sent to {sent} users")

# ================= BAN / UNBAN =================

async def handle_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /ban <user_id>  or  /unban <user_id>")
        return

    target_uid = int(context.args[0])
    is_unban   = update.message.text.startswith("/unban")

    if not await user_exists(target_uid):
        await update.message.reply_text(f"❌ User {target_uid} not found.")
        return

    if is_unban:
        await db_pool.execute("UPDATE users SET is_banned = FALSE WHERE user_id = $1", target_uid)
        await update.message.reply_text(f"✅ User {target_uid} unbanned.")
        try:
            await context.bot.send_message(target_uid, "✅ Your ban has been lifted.")
        except Exception:
            pass
    else:
        await db_pool.execute("UPDATE users SET is_banned = TRUE WHERE user_id = $1", target_uid)
        partner = await get_partner(target_uid)
        if partner:
            await db_pool.execute("DELETE FROM active_chats WHERE user_id = $1", target_uid)
            await db_pool.execute("DELETE FROM active_chats WHERE user_id = $1", partner)
            try:
                await context.bot.send_message(partner, "👋 Stranger left the chat.")
            except Exception:
                pass
        await db_pool.execute("DELETE FROM waiting_users WHERE user_id = $1", target_uid)
        await update.message.reply_text(f"🚫 User {target_uid} banned.")
        try:
            await context.bot.send_message(target_uid, "🚫 You have been banned.")
        except Exception:
            pass

# ================= CLEANUP NULL USERS =================

async def cleanup_null_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return
    rows = await db_pool.fetch(
        "SELECT user_id FROM users WHERE name IS NULL AND user_id != $1", ADMIN_ID
    )
    if not rows:
        await update.message.reply_text("✅ No incomplete registrations found.")
        return
    count = len(rows)
    for row in rows:
        uid = row["user_id"]
        await db_pool.execute("DELETE FROM waiting_users WHERE user_id = $1", uid)
        await db_pool.execute("DELETE FROM active_chats  WHERE user_id = $1", uid)
        await db_pool.execute("DELETE FROM active_chats  WHERE partner_id = $1", uid)
        await db_pool.execute(
            "DELETE FROM reports WHERE reporter_id = $1 OR reported_id = $1", uid
        )
        await db_pool.execute("DELETE FROM users WHERE user_id = $1", uid)
    await update.message.reply_text(f"🗑 Removed {count} incomplete user(s).")

# ================= DEBUG REFERRAL =================

async def debug_referral(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /debugref <user_id>")
        return
    row = await db_pool.fetchrow(
        "SELECT user_id, name, gender, country, age, referred_by, referral_count, is_vip, vip_expiry FROM users WHERE user_id = $1",
        int(context.args[0]),
    )
    if not row:
        await update.message.reply_text("User not found.")
        return
    await update.message.reply_text(
        f"🔍 Debug\n\n"
        f"name:           {row['name']}\n"
        f"gender:         {row['gender']}\n"
        f"country:        {row['country']}\n"
        f"age:            {row['age']}\n"
        f"referred_by:    {row['referred_by']}\n"
        f"referral_count: {row['referral_count']}\n"
        f"is_vip:         {row['is_vip']}\n"
        f"vip_expiry:     {row['vip_expiry']}\n"
    )

# ================= MATCH =================

async def match_user(
    update: Update, context: ContextTypes.DEFAULT_TYPE, pref: str = None
):
    uid = update.message.from_user.id

    if await get_partner(uid):
        await update.message.reply_text("You are already in a chat. Use ❌ Stop first.")
        return

    # Cancel any previous invite timer before starting a new search
    cancel_invite_timer(context)

    await db_pool.execute("DELETE FROM waiting_users WHERE user_id = $1", uid)

    row       = await db_pool.fetchrow("SELECT gender FROM users WHERE user_id = $1", uid)
    my_gender = row["gender"] if row else None

    partner = None

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            if pref:
                candidates_rows = await conn.fetch(
                    """
                    SELECT w.user_id FROM waiting_users w
                    JOIN users u ON w.user_id = u.user_id
                    WHERE u.gender = $1
                      AND (w.preferred_gender IS NULL OR w.preferred_gender = $2)
                      AND w.user_id != $3
                      AND u.is_banned = FALSE
                    ORDER BY w.queued_at LIMIT 5
                    """,
                    pref, my_gender, uid,
                )
            else:
                candidates_rows = await conn.fetch(
                    """
                    SELECT w.user_id FROM waiting_users w
                    JOIN users u ON w.user_id = u.user_id
                    WHERE w.user_id != $1
                      AND (w.preferred_gender IS NULL OR w.preferred_gender = $2)
                      AND u.is_banned = FALSE
                    ORDER BY w.queued_at LIMIT 5
                    """,
                    uid, my_gender,
                )

            candidates = [r["user_id"] for r in candidates_rows]
            random.shuffle(candidates)

            for candidate in candidates:
                locked = await conn.fetchval(
                    "SELECT pg_try_advisory_xact_lock($1)", candidate
                )
                if not locked:
                    continue
                still_waiting = await conn.fetchrow(
                    "SELECT 1 FROM waiting_users WHERE user_id = $1", candidate
                )
                if not still_waiting:
                    continue
                await conn.execute("DELETE FROM waiting_users WHERE user_id = $1", candidate)
                await conn.execute(
                    "INSERT INTO active_chats VALUES ($1,$2) ON CONFLICT (user_id) DO UPDATE SET partner_id=EXCLUDED.partner_id",
                    uid, candidate,
                )
                await conn.execute(
                    "INSERT INTO active_chats VALUES ($1,$2) ON CONFLICT (user_id) DO UPDATE SET partner_id=EXCLUDED.partner_id",
                    candidate, uid,
                )
                partner = candidate
                break

    if partner:
        # Cancel invite timers for both sides
        cancel_invite_timer(context)
        await context.bot.send_message(uid,     "✅ Connected! Say hi 👋")
        await context.bot.send_message(partner, "✅ Connected! Say hi 👋")
    else:
        await db_pool.execute(
            """
            INSERT INTO waiting_users (user_id, preferred_gender)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET preferred_gender = EXCLUDED.preferred_gender
            """,
            uid, pref,
        )
        if pref == "Male":
            await update.message.reply_text("🔎 Searching for a Male partner...")
        elif pref == "Female":
            await update.message.reply_text("🔎 Searching for a Female partner...")
        else:
            await update.message.reply_text("🔎 Searching for a partner...")

        # FIX: Store task handle so it can be cancelled on stop/match
        async def invite_prompt():
            try:
                await asyncio.sleep(MATCH_INVITE_DELAY)
                row = await db_pool.fetchrow(
                    "SELECT 1 FROM waiting_users WHERE user_id = $1", uid
                )
                if row:
                    link = f"https://t.me/{context.bot.username}?start={uid}"
                    await context.bot.send_message(
                        uid,
                        f"🔎 Still searching?\n\n"
                        f"Invite {VIP_REFERRAL_THRESHOLD} friends and unlock "
                        f"👑 VIP for {VIP_REFERRAL_DAYS} days!\n\n"
                        f"Your invite link:\n{link}",
                    )
            except asyncio.CancelledError:
                pass  # Task was cancelled cleanly — do nothing
            finally:
                context.user_data.pop("invite_task", None)

        task = asyncio.create_task(invite_prompt())
        context.user_data["invite_task"] = task

# ================= STOP =================

async def stop_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    uid = update.message.from_user.id

    await db_pool.execute("DELETE FROM waiting_users WHERE user_id = $1", uid)

    # Always cancel invite timer on stop
    cancel_invite_timer(context)

    partner = await get_partner(uid)

    if not partner:
        await update.message.reply_text(
            "⛔ Search stopped.", reply_markup=get_main_keyboard(uid)
        )
        return False

    await db_pool.execute("DELETE FROM active_chats WHERE user_id = $1", uid)
    await db_pool.execute("DELETE FROM active_chats WHERE user_id = $1", partner)

    # partner_id is embedded in the inline button callback_data — no dict needed
    await update.message.reply_text(
        "❌ Chat ended.", reply_markup=get_main_keyboard(uid)
    )
    await update.message.reply_text(
        "Did something go wrong?", reply_markup=report_inline(partner)
    )

    try:
        await context.bot.send_message(
            partner, "👋 Stranger left the chat.",
            reply_markup=get_main_keyboard(partner),
        )
        await context.bot.send_message(
            partner, "Did something go wrong?",
            reply_markup=report_inline(uid),
        )
    except Exception:
        logger.warning("Could not notify partner %s", partner)

    return True

# ================= REPORT CALLBACK =================

async def report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid   = query.from_user.id
    await query.answer()

    try:
        partner = int(query.data.split(":")[1])
    except Exception:
        await query.edit_message_text("⚠️ Invalid report.")
        return

    existing = await db_pool.fetchrow(
        """
        SELECT 1 FROM reports
        WHERE reporter_id = $1 AND reported_id = $2
          AND created_at > NOW() - INTERVAL '1 hour'
        """,
        uid, partner,
    )
    if existing:
        await query.edit_message_text("You already reported this user recently.")
        return

    await db_pool.execute(
        "INSERT INTO reports (reporter_id, reported_id) VALUES ($1, $2)", uid, partner
    )
    report_count = await db_pool.fetchval(
        "SELECT COUNT(*) FROM reports WHERE reported_id = $1", partner
    )
    reported_row = await db_pool.fetchrow(
        "SELECT username, name FROM users WHERE user_id = $1", partner
    )
    name     = (reported_row["name"]     or "Unknown") if reported_row else "Unknown"
    username = (reported_row["username"] or "Unknown") if reported_row else "Unknown"

    try:
        await context.bot.send_message(
            ADMIN_ID,
            f"🚨 New Report\n\n"
            f"Reporter ID:  {uid}\n"
            f"Reported ID:  {partner}\n"
            f"Name:         {name}\n"
            f"Username:     @{username}\n"
            f"Total reports: {report_count}\n\n"
            f"To ban: /ban {partner}",
        )
    except Exception:
        pass

    await query.edit_message_text(
        "✅ Report submitted. The admin has been notified."
    )

# ================= ROUTER =================

BUTTON_TEXTS = {
    "🚀 Find Partner", "👨 Find Male", "👩 Find Female",
    "⏭ Next", "❌ Stop", "💎 VIP", "🎁 Get FREE VIP",
    "👑 Contact Admin", "⬅ Back", "⚙️ Admin Panel",
    "⚠️ Report",
    "📊 Analytics", "👥 Active Users", "🕒 Waiting Users",
    "📢 Announcement", "🧹 Clean Dead Chats",
    "Male", "Female",
}

async def router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    uid  = update.message.from_user.id
    # FIX: only use text for registration/button logic — media still relays fine
    text = update.message.text or ""

    if await is_banned(uid):
        await update.message.reply_text("🚫 You are banned from using this bot.")
        return

    # ── REGISTRATION RECOVERY ──
    # Silently restore step from DB if context was wiped by restart.
    # Does NOT return — just sets the step then falls through to the handler.
    if uid != ADMIN_ID and context.user_data.get("step") is None:
        if await user_exists(uid) and not await is_registered(uid):
            db_step = await get_registration_step(uid)
            if db_step:
                context.user_data["step"] = db_step

    step = context.user_data.get("step")

    # ── REGISTRATION FLOW ──
    # Only intercept text messages during registration.
    # Media (photos, stickers, voice) is ignored here and falls to relay.

    if step == "name":
        if not text or text in BUTTON_TEXTS:
            await update.message.reply_text("Please enter your name:")
            return
        await db_pool.execute("UPDATE users SET name=$1 WHERE user_id=$2", text, uid)
        context.user_data["step"] = "gender"
        await update.message.reply_text(
            "Select your gender:", reply_markup=gender_keyboard
        )
        return

    if step == "gender":
        if text not in ("Male", "Female"):
            await update.message.reply_text(
                "Please select your gender using the buttons.",
                reply_markup=gender_keyboard,
            )
            return
        await db_pool.execute("UPDATE users SET gender=$1 WHERE user_id=$2", text, uid)
        context.user_data["step"] = "country"
        await update.message.reply_text(
            "Enter your country:", reply_markup=ReplyKeyboardRemove()
        )
        return

    if step == "country":
        if not text or text in BUTTON_TEXTS:
            await update.message.reply_text("Please enter your country:")
            return
        await db_pool.execute("UPDATE users SET country=$1 WHERE user_id=$2", text, uid)
        context.user_data["step"] = "age"
        await update.message.reply_text("Enter your age:")
        return

    if step == "age":
        if not text.isdigit() or not (5 <= int(text) <= 120):
            await update.message.reply_text("Please enter a valid age (5–120).")
            return
        await db_pool.execute("UPDATE users SET age=$1 WHERE user_id=$2", int(text), uid)
        context.user_data["step"] = None

        # Referral always from DB — never lost on restart
        ref_row  = await db_pool.fetchrow("SELECT referred_by FROM users WHERE user_id=$1", uid)
        referrer = ref_row["referred_by"] if ref_row else None
        if referrer:
            vip_granted = await handle_referral(uid, referrer)
            if vip_granted:
                try:
                    await context.bot.send_message(
                        referrer,
                        f"🎉 You earned {VIP_REFERRAL_DAYS} days of 👑 VIP for inviting friends!",
                    )
                except Exception:
                    pass

        await update.message.reply_text(
            "Registration complete 🎉\n\nUse the buttons below to find a chat partner!",
            reply_markup=user_keyboard,
        )
        return

    # ── ADMIN PANEL ENTRY ──

    if text == "⚙️ Admin Panel" and uid == ADMIN_ID:
        context.user_data["in_admin_panel"] = True
        await update.message.reply_text("⚙️ Admin Panel", reply_markup=admin_panel_keyboard)
        return

    # ── ADMIN PANEL ACTIONS ──

    if uid == ADMIN_ID and context.user_data.get("in_admin_panel"):

        if text == "📊 Analytics":
            total     = await db_pool.fetchval("SELECT COUNT(*) FROM users")
            active    = (await db_pool.fetchval("SELECT COUNT(*) FROM active_chats") or 0) // 2
            waiting   = await db_pool.fetchval("SELECT COUNT(*) FROM waiting_users")
            vip_count = await db_pool.fetchval(
                "SELECT COUNT(*) FROM users WHERE (is_vip=TRUE AND vip_expiry IS NULL) OR vip_expiry > NOW()"
            )
            banned  = await db_pool.fetchval("SELECT COUNT(*) FROM users WHERE is_banned=TRUE")
            reports = await db_pool.fetchval("SELECT COUNT(*) FROM reports")
            await update.message.reply_text(
                f"📊 Analytics\n\n"
                f"👤 Total users:   {total}\n"
                f"💬 Active chats:  {active}\n"
                f"🔎 Waiting:       {waiting}\n"
                f"👑 Active VIPs:   {vip_count}\n"
                f"🚫 Banned:        {banned}\n"
                f"🚨 Total reports: {reports}"
            )
            return

        if text == "👥 Active Users":
            active = (await db_pool.fetchval("SELECT COUNT(*) FROM active_chats") or 0) // 2
            await update.message.reply_text(f"💬 Active chats: {active}")
            return

        if text == "🕒 Waiting Users":
            waiting = await db_pool.fetchval("SELECT COUNT(*) FROM waiting_users")
            await update.message.reply_text(f"🔎 Waiting users: {waiting}")
            return

        if text == "🧹 Clean Dead Chats":
            await update.message.reply_text("🔍 Scanning for dead chats...")
            cleaned = await clean_dead_chats(context.bot)
            await update.message.reply_text(f"✅ Done. Removed {cleaned} dead chat(s).")
            return

        if text == "📢 Announcement":
            context.user_data["announce_mode"] = True
            await update.message.reply_text("Send the announcement message now:")
            return

        if context.user_data.get("announce_mode"):
            if text == "⬅ Back":
                context.user_data["announce_mode"] = False
                await update.message.reply_text(
                    "Announcement cancelled.", reply_markup=admin_panel_keyboard
                )
                return
            context.user_data["announce_mode"] = False
            rows = await db_pool.fetch("SELECT user_id FROM users")
            sent = 0
            for row in rows:
                try:
                    await update.message.copy(chat_id=row["user_id"])
                    sent += 1
                    await asyncio.sleep(0.05)
                except Exception:
                    pass
            await update.message.reply_text(f"📢 Sent to {sent} users.")
            return

        if text == "⬅ Back":
            context.user_data["in_admin_panel"] = False
            context.user_data.pop("announce_mode", None)
            await update.message.reply_text("Main menu", reply_markup=admin_main_keyboard)
            return

    # ── SHARED BUTTONS ──

    if text == "🚀 Find Partner":
        await match_user(update, context)
        return

    if text == "👨 Find Male":
        if await check_vip(uid):
            await match_user(update, context, "Male")
        else:
            await update.message.reply_text(
                "👑 VIP required to filter by gender.\n\nUse 💎 VIP to learn more."
            )
        return

    if text == "👩 Find Female":
        if await check_vip(uid):
            await match_user(update, context, "Female")
        else:
            await update.message.reply_text(
                "👑 VIP required to filter by gender.\n\nUse 💎 VIP to learn more."
            )
        return

    if text == "⏭ Next":
        await stop_chat(update, context)
        await match_user(update, context)
        return

    if text == "❌ Stop":
        await stop_chat(update, context)
        return

    if text == "💎 VIP":
        is_active = await check_vip(uid)
        status    = "✅ Active" if is_active else "❌ Inactive"
        if is_active:
            row = await db_pool.fetchrow("SELECT vip_expiry FROM users WHERE user_id=$1", uid)
            expiry_str = (
                row["vip_expiry"].strftime("%Y-%m-%d %H:%M UTC")
                if row and row["vip_expiry"] else "Permanent ♾️"
            )
            msg = f"👑 VIP Status: {status}\nExpires: {expiry_str}"
        else:
            msg = f"👑 VIP Status: {status}\n\nVIP lets you filter partners by gender."
        await update.message.reply_text(msg, reply_markup=vip_keyboard)
        return

    if text == "🎁 Get FREE VIP":
        row            = await db_pool.fetchrow(
            "SELECT referral_count FROM users WHERE user_id=$1", uid
        )
        count          = row["referral_count"] if row else 0
        link           = f"https://t.me/{context.bot.username}?start={uid}"
        cycle_progress = count % VIP_REFERRAL_THRESHOLD
        remaining      = VIP_REFERRAL_THRESHOLD - cycle_progress
        total_vips     = count // VIP_REFERRAL_THRESHOLD
        await update.message.reply_text(
            f"🎁 Invite friends to get FREE VIP!\n\n"
            f"Your invite link:\n{link}\n\n"
            f"📊 Total referrals: {count}\n"
            f"🔄 Progress: {cycle_progress}/{VIP_REFERRAL_THRESHOLD}\n"
            f"🏆 VIPs earned: {total_vips}\n\n"
            f"Invite {remaining} more to unlock 👑 VIP for {VIP_REFERRAL_DAYS} days!"
        )
        return

    if text == "👑 Contact Admin":
        await update.message.reply_text("👑 Contact Admin: @Random1204")
        return

    if text == "⬅ Back":
        context.user_data.pop("announce_mode", None)
        await update.message.reply_text("Main menu", reply_markup=get_main_keyboard(uid))
        return

    # ── RELAY MESSAGE / MEDIA TO PARTNER ──
    # Handles text, photos, stickers, voice, video, documents — everything

    if text not in BUTTON_TEXTS:
        partner = await get_partner(uid)
        if partner:
            try:
                await update.message.copy(chat_id=partner)
                if text:  # only count text messages
                    await db_pool.execute(
                        "UPDATE users SET total_messages=total_messages+1 WHERE user_id=$1", uid
                    )
            except Forbidden:
                # Partner blocked the bot — end chat definitively
                logger.warning("Partner %s blocked the bot, ending chat", partner)
                await db_pool.execute("DELETE FROM active_chats WHERE user_id=$1", uid)
                await db_pool.execute("DELETE FROM active_chats WHERE user_id=$1", partner)
                await update.message.reply_text(
                    "⚠️ Partner is unavailable.", reply_markup=get_main_keyboard(uid)
                )
            except TimedOut:
                # Temporary network issue — do NOT end the chat
                logger.warning("Timeout relaying to partner %s, ignoring", partner)
            except Exception as e:
                logger.warning("Relay error to %s: %s", partner, e)
                await db_pool.execute("DELETE FROM active_chats WHERE user_id=$1", uid)
                await db_pool.execute("DELETE FROM active_chats WHERE user_id=$1", partner)
                await update.message.reply_text(
                    "⚠️ Partner disconnected.", reply_markup=get_main_keyboard(uid)
                )
        else:
            await update.message.reply_text(
                "You are not in a chat. Press 🚀 Find Partner to start.",
                reply_markup=get_main_keyboard(uid),
            )

# ================= START =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid      = update.message.from_user.id
    username = update.message.from_user.username

    if await is_banned(uid):
        await update.message.reply_text("🚫 You are banned from using this bot.")
        return

    ref = None
    if context.args and context.args[0].isdigit():
        ref = int(context.args[0])
        if ref == uid:
            ref = None

    if uid == ADMIN_ID:
        await db_pool.execute(
            """
            INSERT INTO users (user_id, username, name, gender, is_vip)
            VALUES ($1, $2, 'Admin', 'Male', TRUE)
            ON CONFLICT (user_id) DO NOTHING
            """,
            uid, username,
        )
        await db_pool.execute("DELETE FROM active_chats WHERE user_id=$1", uid)
        await db_pool.execute("DELETE FROM waiting_users WHERE user_id=$1", uid)
        context.user_data.clear()
        await update.message.reply_text("Welcome, Admin 👋", reply_markup=admin_main_keyboard)
        return

    if await user_exists(uid):
        if not await is_registered(uid):
            context.user_data.clear()
            context.user_data["step"] = "name"
            await update.message.reply_text(
                "Let's finish your profile.\n\nEnter your name:"
            )
        else:
            await update.message.reply_text("Welcome back! 👋", reply_markup=user_keyboard)
        return

    # New user — save referred_by to DB immediately
    await db_pool.execute(
        "INSERT INTO users (user_id, username, referred_by) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
        uid, username, ref,
    )
    context.user_data.clear()
    context.user_data["step"] = "name"
    await update.message.reply_text(
        "👋 Welcome! Let's set up your profile.\n\nEnter your name:"
    )

# ================= MAIN =================

async def main():
    global db_pool

    dsn = DATABASE_URL
    if dsn and dsn.startswith("postgres://"):
        dsn = dsn.replace("postgres://", "postgresql://", 1)

    db_pool = await asyncpg.create_pool(dsn, min_size=2, max_size=20)
    await init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",      start))
    app.add_handler(CommandHandler("broadcast",  broadcast))
    app.add_handler(CommandHandler("ban",        handle_ban))
    app.add_handler(CommandHandler("unban",      handle_ban))
    app.add_handler(CommandHandler("cleanchats", cleanchats_command))
    app.add_handler(CommandHandler("debugref",   debug_referral))
    app.add_handler(CommandHandler("cleanup",    cleanup_null_users))
    app.add_handler(CallbackQueryHandler(report_callback, pattern=r"^report:"))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, router))

    logger.info("Bot started")
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    try:
        await asyncio.Event().wait()
    finally:
        # Graceful shutdown — always close the pool
        logger.info("Shutting down, closing DB pool...")
        await db_pool.close()
        await app.stop()
        await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
