import os
import random
import logging
import asyncio
from datetime import datetime, timezone
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool

from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
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

# ================= CONNECTION POOL =================

db_pool = pool.ThreadedConnectionPool(1, 20, DATABASE_URL)

@contextmanager
def get_db():
    conn = db_pool.getconn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            yield conn, cur
    except Exception:
        logger.exception("Database error")
        raise
    finally:
        db_pool.putconn(conn)

# ================= SCHEMA =================

def init_db():
    raw_conn = db_pool.getconn()
    raw_conn.autocommit = False
    try:
        with raw_conn.cursor() as cur:

            # 1. Users table
            cur.execute("""
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
                    created_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)

            # 2. Waiting users table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS waiting_users (
                    user_id          BIGINT PRIMARY KEY,
                    preferred_gender TEXT
                )
            """)

            # 3. Active chats table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS active_chats (
                    user_id    BIGINT PRIMARY KEY,
                    partner_id BIGINT
                )
            """)

        # Commit tables first
        raw_conn.commit()

        with raw_conn.cursor() as cur:
            # 4. Migrate: add queued_at if this is an existing DB
            cur.execute("""
                ALTER TABLE waiting_users
                ADD COLUMN IF NOT EXISTS queued_at
                TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            """)

        # Commit column before creating index that depends on it
        raw_conn.commit()

        with raw_conn.cursor() as cur:
            # 5. Indexes
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_waiting_queue
                ON waiting_users (queued_at)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_vip_expiry
                ON users (vip_expiry)
            """)

        raw_conn.commit()

    except Exception:
        raw_conn.rollback()
        logger.exception("Database init failed")
        raise
    finally:
        db_pool.putconn(raw_conn)

    logger.info("Database initialised and migrated successfully")

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

vip_keyboard = ReplyKeyboardMarkup(
    [
        ["🎁 Get FREE VIP"],
        ["👑 Contact Admin"],
        ["⬅ Back"],
    ],
    resize_keyboard=True,
)

admin_keyboard = ReplyKeyboardMarkup(
    [
        ["📊 Analytics"],
        ["📢 Announcement"],
        ["👥 Active Users", "🕒 Waiting Users"],
        ["👑 VIP Toggle"],
        ["🧹 Clean Dead Chats"],
        ["⬅ Back"],
    ],
    resize_keyboard=True,
)

gender_keyboard = ReplyKeyboardMarkup(
    [["Male", "Female"]],
    resize_keyboard=True,
    one_time_keyboard=True,
)

# ================= HELPERS =================

def user_exists(uid: int) -> bool:
    with get_db() as (_, cur):
        cur.execute("SELECT 1 FROM users WHERE user_id = %s", (uid,))
        return cur.fetchone() is not None


def is_registered(uid: int) -> bool:
    with get_db() as (_, cur):
        cur.execute(
            "SELECT 1 FROM users WHERE user_id = %s AND name IS NOT NULL AND gender IS NOT NULL",
            (uid,),
        )
        return cur.fetchone() is not None


def get_partner(uid: int):
    with get_db() as (_, cur):
        cur.execute("SELECT partner_id FROM active_chats WHERE user_id = %s", (uid,))
        row = cur.fetchone()
        return row[0] if row else None


def check_vip(uid: int) -> bool:
    with get_db() as (_, cur):
        cur.execute("SELECT is_vip, vip_expiry FROM users WHERE user_id = %s", (uid,))
        row = cur.fetchone()
        if not row:
            return False
        is_vip, expiry = row
        # Permanent VIP — manually granted with no expiry
        if is_vip and expiry is None:
            return True
        # Timed VIP — check expiry
        if expiry and expiry > datetime.now(timezone.utc):
            return True
        return False


def grant_vip(uid: int, days: int):
    with get_db() as (_, cur):
        cur.execute(
            """
            UPDATE users
            SET is_vip     = TRUE,
                vip_expiry = GREATEST(NOW(), COALESCE(vip_expiry, NOW()))
                             + (%s || ' days')::INTERVAL
            WHERE user_id = %s
            RETURNING vip_expiry
            """,
            (days, uid),
        )
        row = cur.fetchone()
        return row[0] if row else None


def revoke_vip(uid: int):
    with get_db() as (_, cur):
        cur.execute(
            "UPDATE users SET is_vip = FALSE, vip_expiry = NULL WHERE user_id = %s",
            (uid,),
        )


def handle_referral(new_uid: int, referrer_uid: int) -> bool:
    if referrer_uid == new_uid:
        return False
    if not is_registered(new_uid):
        return False
    with get_db() as (_, cur):
        cur.execute(
            """
            UPDATE users
            SET referral_count = referral_count + 1
            WHERE user_id = %s
            RETURNING referral_count
            """,
            (referrer_uid,),
        )
        row = cur.fetchone()
        if row and row[0] % VIP_REFERRAL_THRESHOLD == 0:
            grant_vip(referrer_uid, VIP_REFERRAL_DAYS)
            return True
    return False

async def clean_dead_chats(context: ContextTypes.DEFAULT_TYPE = None, bot=None):
    """
    Checks every active chat by sending a dummy getChatMember request.
    If both users in a pair are unreachable, the chat row is deleted.
    Safe to call manually via /cleanchats or automatically on startup.
    """
    b = bot or (context.bot if context else None)
    if not b:
        return 0

    with get_db() as (_, cur):
        cur.execute("SELECT DISTINCT user_id, partner_id FROM active_chats")
        pairs = cur.fetchall()

    seen   = set()
    cleaned = 0

    for uid, partner in pairs:
        pair_key = tuple(sorted((uid, partner)))
        if pair_key in seen:
            continue
        seen.add(pair_key)

        uid_alive     = False
        partner_alive = False

        try:
            await b.send_chat_action(chat_id=uid, action="typing")
            uid_alive = True
        except Exception:
            pass

        try:
            await b.send_chat_action(chat_id=partner, action="typing")
            partner_alive = True
        except Exception:
            pass

        if not uid_alive or not partner_alive:
            with get_db() as (_, cur):
                cur.execute("DELETE FROM active_chats WHERE user_id = %s", (uid,))
                cur.execute("DELETE FROM active_chats WHERE user_id = %s", (partner,))
            cleaned += 1
            logger.info("Cleaned dead chat between %s and %s", uid, partner)

            # Notify the still-alive side
            if uid_alive:
                try:
                    await b.send_message(uid, "⚠️ Your partner disconnected. Press 🚀 Find Partner to start again.")
                except Exception:
                    pass
            if partner_alive:
                try:
                    await b.send_message(partner, "⚠️ Your partner disconnected. Press 🚀 Find Partner to start again.")
                except Exception:
                    pass

    return cleaned


async def cleanchats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return
    await update.message.reply_text("🔍 Scanning for dead chats...")
    cleaned = await clean_dead_chats(bot=context.bot)
    await update.message.reply_text(f"✅ Done. Removed {cleaned} dead chat(s).")


# ================= BROADCAST =================

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    msg = " ".join(context.args)
    with get_db() as (_, cur):
        cur.execute("SELECT user_id FROM users")
        users = cur.fetchall()
    sent = 0
    for (user_id,) in users:
        try:
            await context.bot.send_message(user_id, msg)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass
    await update.message.reply_text(f"✅ Sent to {sent} users")

# ================= VIP TOGGLE =================

async def handle_vip_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return
    args = context.args
    if not args or not args[0].isdigit():
        await update.message.reply_text(
            "Usage:\n"
            "/vip <user_id>         → grant 30 days\n"
            "/vip <user_id> <days>  → grant N days\n"
            "/vip <user_id> 0       → revoke VIP"
        )
        return
    target_uid = int(args[0])
    days = int(args[1]) if len(args) > 1 and args[1].isdigit() else 30
    if not user_exists(target_uid):
        await update.message.reply_text(f"❌ User {target_uid} not found.")
        return
    if days == 0:
        revoke_vip(target_uid)
        await update.message.reply_text(f"✅ VIP revoked for user {target_uid}.")
        try:
            await context.bot.send_message(target_uid, "ℹ️ Your VIP has been removed by an admin.")
        except Exception:
            pass
    else:
        expiry = grant_vip(target_uid, days)
        expiry_str = expiry.strftime("%Y-%m-%d %H:%M UTC") if expiry else "unknown"
        await update.message.reply_text(
            f"✅ Granted {days} days VIP to user {target_uid}.\n"
            f"Expires: {expiry_str}"
        )
        try:
            await context.bot.send_message(
                target_uid,
                f"🎉 An admin has granted you 👑 VIP for {days} days!\n"
                f"Expires: {expiry_str}",
            )
        except Exception:
            pass

# ================= MATCH =================

async def match_user(update: Update, context: ContextTypes.DEFAULT_TYPE, pref: str = None):
    uid = update.message.from_user.id

    if get_partner(uid):
        await update.message.reply_text("You are already in a chat. Use ❌ Stop first.")
        return

    with get_db() as (_, cur):
        cur.execute("DELETE FROM waiting_users WHERE user_id = %s", (uid,))

    with get_db() as (_, cur):
        cur.execute("SELECT gender FROM users WHERE user_id = %s", (uid,))
        row = cur.fetchone()
        my_gender = row[0] if row else None

    partner = None

    raw_conn = db_pool.getconn()
    raw_conn.autocommit = False
    try:
        with raw_conn.cursor() as cur:
            if pref:
                cur.execute(
                    """
                    SELECT w.user_id
                    FROM waiting_users w
                    JOIN users u ON w.user_id = u.user_id
                    WHERE u.gender = %s
                      AND (w.preferred_gender IS NULL OR w.preferred_gender = %s)
                      AND w.user_id != %s
                    ORDER BY w.queued_at
                    LIMIT 5
                    """,
                    (pref, my_gender, uid),
                )
            else:
                cur.execute(
                    """
                    SELECT w.user_id
                    FROM waiting_users w
                    JOIN users u ON w.user_id = u.user_id
                    WHERE w.user_id != %s
                      AND (
                          w.preferred_gender IS NULL
                          OR w.preferred_gender = %s
                      )
                    ORDER BY w.queued_at
                    LIMIT 5
                    """,
                    (uid, my_gender),
                )

            candidates = [r[0] for r in cur.fetchall()]
            random.shuffle(candidates)

            for candidate in candidates:
                cur.execute("SELECT pg_try_advisory_xact_lock(%s)", (candidate,))
                if not cur.fetchone()[0]:
                    continue
                cur.execute("SELECT 1 FROM waiting_users WHERE user_id = %s", (candidate,))
                if not cur.fetchone():
                    continue
                cur.execute("DELETE FROM waiting_users WHERE user_id = %s", (candidate,))
                cur.execute(
                    "INSERT INTO active_chats VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET partner_id = EXCLUDED.partner_id",
                    (uid, candidate),
                )
                cur.execute(
                    "INSERT INTO active_chats VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET partner_id = EXCLUDED.partner_id",
                    (candidate, uid),
                )
                partner = candidate
                break

        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        logger.exception("Match transaction failed for uid=%s", uid)
    finally:
        db_pool.putconn(raw_conn)

    if partner:
        context.user_data["invite_timer"] = False
        await context.bot.send_message(uid,     "✅ Connected! Say hi 👋")
        await context.bot.send_message(partner, "✅ Connected! Say hi 👋")
    else:
        with get_db() as (_, cur):
            cur.execute(
                """
                INSERT INTO waiting_users (user_id, preferred_gender)
                VALUES (%s, %s)
                ON CONFLICT (user_id) DO UPDATE SET preferred_gender = EXCLUDED.preferred_gender
                """,
                (uid, pref),
            )
        await update.message.reply_text("🔎 Searching for a partner...")

        if not context.user_data.get("invite_timer"):
            context.user_data["invite_timer"] = True

            async def invite_prompt():
                await asyncio.sleep(MATCH_INVITE_DELAY)
                with get_db() as (_, cur):
                    cur.execute("SELECT 1 FROM waiting_users WHERE user_id = %s", (uid,))
                    still_waiting = cur.fetchone()
                if still_waiting:
                    link = f"https://t.me/{context.bot.username}?start={uid}"
                    await context.bot.send_message(
                        uid,
                        f"🔎 Still searching?\n\n"
                        f"Invite {VIP_REFERRAL_THRESHOLD} friends and unlock "
                        f"👑 VIP for {VIP_REFERRAL_DAYS} days!\n\n"
                        f"Your invite link:\n{link}",
                    )
                context.user_data["invite_timer"] = False

            asyncio.create_task(invite_prompt())

# ================= STOP =================

async def stop_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    uid = update.message.from_user.id

    with get_db() as (_, cur):
        cur.execute("DELETE FROM waiting_users WHERE user_id = %s", (uid,))

    context.user_data["invite_timer"] = False
    partner = get_partner(uid)

    if not partner:
        await update.message.reply_text("⛔ Search stopped.", reply_markup=user_keyboard)
        return False

    with get_db() as (_, cur):
        cur.execute("DELETE FROM active_chats WHERE user_id = %s", (uid,))
        cur.execute("DELETE FROM active_chats WHERE user_id = %s", (partner,))

    await update.message.reply_text("❌ Chat ended.", reply_markup=user_keyboard)

    try:
        await context.bot.send_message(partner, "👋 Stranger left the chat.")
    except Exception:
        logger.warning("Could not notify partner %s", partner)

    return True

# ================= ROUTER =================

# All buttons that should NOT be relayed to chat partner
BUTTON_TEXTS = {
    "🚀 Find Partner", "👨 Find Male", "👩 Find Female",
    "⏭ Next", "❌ Stop", "💎 VIP", "🎁 Get FREE VIP",
    "👑 Contact Admin", "⬅ Back",
    "📊 Analytics", "👥 Active Users", "🕒 Waiting Users",
    "📢 Announcement", "👑 VIP Toggle", "🧹 Clean Dead Chats",
}

async def router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    uid  = update.message.from_user.id
    text = update.message.text or ""

    # -------- REGISTRATION FLOW --------

    step = context.user_data.get("step")

    if step == "name":
        context.user_data["name"] = text
        context.user_data["step"] = "gender"
        await update.message.reply_text("Select your gender:", reply_markup=gender_keyboard)
        return

    if step == "gender":
        if text not in ("Male", "Female"):
            await update.message.reply_text("Please select your gender using the buttons.")
            return
        context.user_data["gender"] = text
        context.user_data["step"]   = "country"
        await update.message.reply_text("Enter your country:")
        return

    if step == "country":
        context.user_data["country"] = text
        context.user_data["step"]    = "age"
        await update.message.reply_text("Enter your age:")
        return

    if step == "age":
        if not text.isdigit() or not (5 <= int(text) <= 120):
            await update.message.reply_text("Please enter a valid age.")
            return
        with get_db() as (_, cur):
            cur.execute(
                "UPDATE users SET name=%s, gender=%s, country=%s, age=%s WHERE user_id=%s",
                (
                    context.user_data["name"],
                    context.user_data["gender"],
                    context.user_data["country"],
                    int(text),
                    uid,
                ),
            )
        context.user_data["step"] = None
        referrer = context.user_data.pop("pending_referrer", None)
        if referrer:
            vip_granted = handle_referral(uid, referrer)
            if vip_granted:
                try:
                    await context.bot.send_message(
                        referrer,
                        f"🎉 You earned {VIP_REFERRAL_DAYS} days of 👑 VIP for inviting friends!",
                    )
                except Exception:
                    pass
        await update.message.reply_text("Registration complete 🎉", reply_markup=user_keyboard)
        return

    # -------- ADMIN-ONLY BUTTONS --------
    # Only buttons that ONLY exist in the admin panel go here.
    # Shared buttons (Find Partner, VIP, etc.) are handled further below
    # so both admin and regular users can use them.

    if uid == ADMIN_ID:

        if text == "📊 Analytics":
            with get_db() as (_, cur):
                cur.execute("SELECT COUNT(*) FROM users")
                total = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM active_chats")
                active = cur.fetchone()[0] // 2
                cur.execute("SELECT COUNT(*) FROM waiting_users")
                waiting = cur.fetchone()[0]
                cur.execute("""
                    SELECT COUNT(*) FROM users
                    WHERE (is_vip = TRUE AND vip_expiry IS NULL)
                       OR vip_expiry > NOW()
                """)
                vip_count = cur.fetchone()[0]
            await update.message.reply_text(
                f"📊 Analytics\n\n"
                f"👤 Total users:   {total}\n"
                f"💬 Active chats:  {active}\n"
                f"🔎 Waiting:       {waiting}\n"
                f"👑 Active VIPs:   {vip_count}"
            )
            return

        if text == "👥 Active Users":
            with get_db() as (_, cur):
                cur.execute("SELECT COUNT(*) FROM active_chats")
                active = cur.fetchone()[0] // 2
            await update.message.reply_text(f"💬 Active chats: {active}")
            return

        if text == "🕒 Waiting Users":
            with get_db() as (_, cur):
                cur.execute("SELECT COUNT(*) FROM waiting_users")
                waiting = cur.fetchone()[0]
            await update.message.reply_text(f"🔎 Waiting users: {waiting}")
            return

        if text == "🧹 Clean Dead Chats":
            await update.message.reply_text("🔍 Scanning for dead chats...")
            cleaned = await clean_dead_chats(bot=context.bot)
            await update.message.reply_text(f"✅ Done. Removed {cleaned} dead chat(s).")
            return

        if text == "👑 VIP Toggle":
            await update.message.reply_text(
                "Use the /vip command to manage VIP:\n\n"
                "/vip <user_id>         → grant 30 days\n"
                "/vip <user_id> <days>  → grant N days\n"
                "/vip <user_id> 0       → revoke VIP"
            )
            return

        if text == "📢 Announcement":
            context.user_data["announce_mode"] = True
            await update.message.reply_text("Send the announcement message now:")
            return

        if context.user_data.get("announce_mode"):
            if text == "⬅ Back":
                context.user_data["announce_mode"] = False
                await update.message.reply_text("Announcement cancelled.", reply_markup=admin_keyboard)
                return
            context.user_data["announce_mode"] = False
            with get_db() as (_, cur):
                cur.execute("SELECT user_id FROM users")
                users = cur.fetchall()
            sent = 0
            for (user_id,) in users:
                try:
                    await update.message.copy(chat_id=user_id)
                    sent += 1
                    await asyncio.sleep(0.05)
                except Exception:
                    pass
            await update.message.reply_text(f"📢 Announcement sent to {sent} users.")
            return

        if text == "⬅ Back":
            await update.message.reply_text("Main menu", reply_markup=user_keyboard)
            return

    # -------- BUTTONS SHARED BY ADMIN AND USERS --------

    if text == "🚀 Find Partner":
        await match_user(update, context)
        return

    if text == "👨 Find Male":
        if check_vip(uid):
            await match_user(update, context, "Male")
        else:
            await update.message.reply_text(
                "👑 VIP required to filter by gender.\n\nUse 💎 VIP to learn more."
            )
        return

    if text == "👩 Find Female":
        if check_vip(uid):
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
        is_active = check_vip(uid)
        status    = "✅ Active" if is_active else "❌ Inactive"
        if is_active:
            with get_db() as (_, cur):
                cur.execute("SELECT vip_expiry FROM users WHERE user_id = %s", (uid,))
                row = cur.fetchone()
                expiry_str = row[0].strftime("%Y-%m-%d %H:%M UTC") if row and row[0] else "Permanent ♾️"
            msg = f"👑 VIP Status: {status}\nExpires: {expiry_str}"
        else:
            msg = f"👑 VIP Status: {status}\n\nVIP lets you filter partners by gender."
        await update.message.reply_text(msg, reply_markup=vip_keyboard)
        return

    if text == "🎁 Get FREE VIP":
        with get_db() as (_, cur):
            cur.execute("SELECT referral_count FROM users WHERE user_id = %s", (uid,))
            row   = cur.fetchone()
            count = row[0] if row else 0
        link      = f"https://t.me/{context.bot.username}?start={uid}"
        progress  = count % VIP_REFERRAL_THRESHOLD
        remaining = VIP_REFERRAL_THRESHOLD - progress
        await update.message.reply_text(
            f"🎁 Invite friends to get FREE VIP!\n\n"
            f"Your link:\n{link}\n\n"
            f"Progress: {progress}/{VIP_REFERRAL_THRESHOLD}\n"
            f"Invite {remaining} more friend(s) to unlock "
            f"👑 VIP for {VIP_REFERRAL_DAYS} days!"
        )
        return

    if text == "👑 Contact Admin":
        await update.message.reply_text("👑 Contact Admin: @Random1204")
        return

    if text == "⬅ Back":
        context.user_data.pop("announce_mode", None)
        await update.message.reply_text("Main menu", reply_markup=user_keyboard)
        return

    # -------- RELAY MESSAGE TO PARTNER --------

    if text not in BUTTON_TEXTS:
        partner = get_partner(uid)
        if partner:
            try:
                await update.message.copy(chat_id=partner)
                with get_db() as (_, cur):
                    cur.execute(
                        "UPDATE users SET total_messages = total_messages + 1 WHERE user_id = %s",
                        (uid,),
                    )
            except Exception:
                logger.warning("Partner %s unreachable, ending chat", partner)
                with get_db() as (_, cur):
                    cur.execute("DELETE FROM active_chats WHERE user_id = %s", (uid,))
                    cur.execute("DELETE FROM active_chats WHERE user_id = %s", (partner,))
                await update.message.reply_text(
                    "⚠️ Partner disconnected.", reply_markup=user_keyboard
                )
        else:
            await update.message.reply_text(
                "You are not in a chat. Press 🚀 Find Partner to start.",
                reply_markup=user_keyboard,
            )

# ================= START =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid      = update.message.from_user.id
    username = update.message.from_user.username

    ref = None
    if context.args and context.args[0].isdigit():
        ref = int(context.args[0])
        if ref == uid:
            ref = None

    if uid == ADMIN_ID:
        # Ensure admin has a users row so match/vip/chat functions work
        with get_db() as (_, cur):
            cur.execute(
                """
                INSERT INTO users (user_id, username, name, gender, is_vip)
                VALUES (%s, %s, 'Admin', 'Male', TRUE)
                ON CONFLICT (user_id) DO NOTHING
                """,
                (uid, username),
            )
        await update.message.reply_text("Welcome, Admin 👋", reply_markup=admin_keyboard)
        return

    if user_exists(uid):
        await update.message.reply_text("Welcome back! 👋", reply_markup=user_keyboard)
        return

    with get_db() as (_, cur):
        cur.execute(
            "INSERT INTO users (user_id, username, referred_by) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (uid, username, ref),
        )

    if ref:
        context.user_data["pending_referrer"] = ref

    context.user_data["step"] = "name"
    await update.message.reply_text(
        "👋 Welcome! Let's set up your profile.\n\nEnter your name:"
    )

# ================= RUN =================

if __name__ == "__main__":
    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",      start))
    app.add_handler(CommandHandler("broadcast",  broadcast))
    app.add_handler(CommandHandler("vip",        handle_vip_toggle))
    app.add_handler(CommandHandler("cleanchats", cleanchats_command))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, router))

    logger.info("Bot started")
    app.run_polling(drop_pending_updates=True)
