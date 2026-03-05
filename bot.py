import os
import psycopg2
import asyncio
from datetime import datetime, timedelta
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters

# ================= CONFIG =================

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = 643086953

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not set")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set")

# ================= DATABASE =================

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    username TEXT,
    name TEXT,
    gender TEXT,
    country TEXT,
    is_vip BOOLEAN DEFAULT FALSE,
    vip_expiry TIMESTAMP,
    referral_count INT DEFAULT 0,
    referred_by BIGINT,
    total_messages INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS waiting_users (
    user_id BIGINT PRIMARY KEY,
    preferred_gender TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS active_chats (
    user_id BIGINT PRIMARY KEY,
    partner_id BIGINT
)
""")

# ================= KEYBOARDS =================

user_keyboard = ReplyKeyboardMarkup(
    [
        ["🚀 Find Partner"],
        ["👨 Find Male", "👩 Find Female"],
        ["⏭ Next", "❌ Stop"],
        ["💎 VIP"]
    ],
    resize_keyboard=True
)

vip_keyboard = ReplyKeyboardMarkup(
    [
        ["🎁 Get FREE VIP"],
        ["👑 Contact Admin"],
        ["⬅ Back"]
    ],
    resize_keyboard=True
)

admin_keyboard = ReplyKeyboardMarkup(
    [
        ["📊 Analytics"],
        ["📢 Announcement"],
        ["👥 Active Users", "🕒 Waiting Users"],
        ["⬅ Back"]
    ],
    resize_keyboard=True
)

gender_keyboard = ReplyKeyboardMarkup(
    [["Male", "Female"]],
    resize_keyboard=True,
    one_time_keyboard=True
)

# ================= HELPERS =================

def user_exists(uid):
    cursor.execute("SELECT 1 FROM users WHERE user_id=%s", (uid,))
    return cursor.fetchone() is not None


def get_partner(uid):
    cursor.execute("SELECT partner_id FROM active_chats WHERE user_id=%s", (uid,))
    row = cursor.fetchone()
    return row[0] if row else None


def is_vip(uid):
    cursor.execute("SELECT is_vip, vip_expiry FROM users WHERE user_id=%s", (uid,))
    row = cursor.fetchone()
    if not row:
        return False
    vip, expiry = row
    return vip or (expiry and expiry > datetime.utcnow())


def reward_referral(uid):
    cursor.execute("SELECT referral_count FROM users WHERE user_id=%s", (uid,))
    row = cursor.fetchone()
    if row and row[0] >= 3:
        expiry = datetime.utcnow() + timedelta(days=3)
        cursor.execute("UPDATE users SET vip_expiry=%s WHERE user_id=%s", (expiry, uid))


# ================= ADMIN BROADCAST =================

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if update.message.from_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("Usage: /broadcast message")
        return

    msg = " ".join(context.args)

    cursor.execute("SELECT user_id FROM users")
    users = cursor.fetchall()

    sent = 0

    for user in users:
        try:
            await context.bot.send_message(user[0], msg)
            sent += 1
            await asyncio.sleep(0.05)
        except:
            pass

    await update.message.reply_text(f"Sent to {sent} users")


# ================= USER MATCH =================

async def match_user(update, context, pref=None):

    uid = update.message.from_user.id

    if get_partner(uid):
        return

    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (uid,))

    if pref:
        cursor.execute("""
        SELECT w.user_id FROM waiting_users w
        JOIN users u ON w.user_id=u.user_id
        WHERE u.gender=%s AND w.user_id!=%s LIMIT 1
        """, (pref, uid))
    else:
        cursor.execute(
            "SELECT user_id FROM waiting_users WHERE user_id!=%s LIMIT 1",
            (uid,)
        )

    row = cursor.fetchone()

    if row:

        partner = row[0]

        cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (partner,))

        cursor.execute("INSERT INTO active_chats VALUES(%s,%s)", (uid, partner))
        cursor.execute("INSERT INTO active_chats VALUES(%s,%s)", (partner, uid))

        await context.bot.send_message(uid, "✅ Connected!")
        await context.bot.send_message(partner, "✅ Connected!")

    else:

        cursor.execute("INSERT INTO waiting_users VALUES(%s,%s)", (uid, pref))
        await update.message.reply_text("🔎 Searching...")


# ================= STOP =================

async def stop_chat(update, context):

    uid = update.message.from_user.id

    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (uid,))

    partner = get_partner(uid)

    if not partner:

        await update.message.reply_text(
            "Search stopped",
            reply_markup=user_keyboard
        )
        return

    cursor.execute("DELETE FROM active_chats WHERE user_id=%s", (uid,))
    cursor.execute("DELETE FROM active_chats WHERE user_id=%s", (partner,))

    await update.message.reply_text(
        "❌ Chat ended",
        reply_markup=user_keyboard
    )

    try:
        await context.bot.send_message(partner, "Stranger left the chat")
    except:
        pass


# ================= ROUTER =================

async def router(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not update.message:
        return

    uid = update.message.from_user.id
    text = update.message.text or ""

    # ================= ADMIN =================

    if uid == ADMIN_ID:

        if text == "📢 Announcement":
            context.user_data["announce_mode"] = True
            await update.message.reply_text("Send announcement message")
            return

        if context.user_data.get("announce_mode"):

            context.user_data["announce_mode"] = False

            cursor.execute("SELECT user_id FROM users")
            users = cursor.fetchall()

            sent = 0

            for user in users:
                try:
                    await update.message.copy(chat_id=user[0])
                    sent += 1
                    await asyncio.sleep(0.05)
                except:
                    pass

            await update.message.reply_text(
                f"✅ Announcement sent to {sent} users"
            )
            return

        if text == "📊 Analytics":

            cursor.execute("SELECT COUNT(*) FROM users")
            total = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM active_chats")
            chats = cursor.fetchone()[0] // 2

            await update.message.reply_text(
                f"Users: {total}\nActive chats: {chats}"
            )
            return

        if text == "👥 Active Users":

            cursor.execute("SELECT COUNT(*) FROM active_chats")
            chats = cursor.fetchone()[0] // 2

            await update.message.reply_text(f"Active chats: {chats}")
            return

        if text == "🕒 Waiting Users":

            cursor.execute("SELECT COUNT(*) FROM waiting_users")
            waiting = cursor.fetchone()[0]

            await update.message.reply_text(f"Waiting users: {waiting}")
            return

    # ================= USER BUTTONS =================

    if text == "🚀 Find Partner":
        await match_user(update, context)

    elif text == "👨 Find Male":
        if is_vip(uid):
            await match_user(update, context, "Male")
        else:
            await update.message.reply_text("👑 VIP required")

    elif text == "👩 Find Female":
        if is_vip(uid):
            await match_user(update, context, "Female")
        else:
            await update.message.reply_text("👑 VIP required")

    elif text == "⏭ Next":
        await stop_chat(update, context)
        await match_user(update, context)

    elif text == "❌ Stop":
        await stop_chat(update, context)

    elif text == "💎 VIP":
        await update.message.reply_text(
            "VIP Menu",
            reply_markup=vip_keyboard
        )

    # ================= MESSAGE FORWARD =================

    partner = get_partner(uid)

    if partner:
        try:
            await update.message.copy(chat_id=partner)
        except:
            pass


# ================= START =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):

    uid = update.message.from_user.id

    if uid == ADMIN_ID:
        await update.message.reply_text(
            "Admin panel",
            reply_markup=admin_keyboard
        )
        return

    if user_exists(uid):

        await update.message.reply_text(
            "Welcome back",
            reply_markup=user_keyboard
        )
        return

    username = update.message.from_user.username
    name = update.message.from_user.first_name

    cursor.execute("""
    INSERT INTO users (user_id,username,name)
    VALUES(%s,%s,%s)
    """,(uid,username,name))

    await update.message.reply_text(
        "Welcome!",
        reply_markup=user_keyboard
    )


# ================= RUN =================

app = ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("broadcast", broadcast))
app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, router))

app.run_polling(drop_pending_updates=True)
