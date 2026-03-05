import os
import psycopg2
import asyncio
from datetime import datetime, timedelta, UTC
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = 643086953

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cursor = conn.cursor()

# ================= DATABASE =================

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
user_id BIGINT PRIMARY KEY,
username TEXT,
name TEXT,
gender TEXT,
country TEXT,
age INT,
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
["👨 Find Male","👩 Find Female"],
["⏭ Next","❌ Stop"],
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
["👥 Active Users","🕒 Waiting Users"],
["⬅ Back"]
],
resize_keyboard=True
)

gender_keyboard = ReplyKeyboardMarkup(
[
["Male","Female"]
],
resize_keyboard=True,
one_time_keyboard=True
)

# ================= HELPERS =================

def user_exists(uid):
    cursor.execute("SELECT 1 FROM users WHERE user_id=%s",(uid,))
    return cursor.fetchone() is not None

def get_partner(uid):
    cursor.execute("SELECT partner_id FROM active_chats WHERE user_id=%s",(uid,))
    row=cursor.fetchone()
    return row[0] if row else None

def is_vip(uid):
    cursor.execute("SELECT is_vip,vip_expiry FROM users WHERE user_id=%s",(uid,))
    row=cursor.fetchone()
    if not row:
        return False
    vip,expiry=row
    return vip or (expiry and expiry > datetime.now(UTC))

# ================= MATCH =================

async def match_user(update,context,pref=None):

    uid=update.message.from_user.id

    # cleanup ghost chats
    partner=get_partner(uid)
    if partner:
        cursor.execute("DELETE FROM active_chats WHERE user_id=%s",(uid,))
        cursor.execute("DELETE FROM active_chats WHERE user_id=%s",(partner,))

    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s",(uid,))

    if pref:
        cursor.execute("""
        SELECT w.user_id
        FROM waiting_users w
        JOIN users u ON w.user_id=u.user_id
        WHERE u.gender=%s
        AND (w.preferred_gender=%s OR w.preferred_gender IS NULL)
        AND w.user_id!=%s
        LIMIT 1
        """,(pref,pref,uid))
    else:
        cursor.execute("""
        SELECT user_id
        FROM waiting_users
        WHERE user_id!=%s
        LIMIT 1
        """,(uid,))

    row=cursor.fetchone()

    if row:

        partner=row[0]

        cursor.execute("DELETE FROM waiting_users WHERE user_id=%s",(partner,))

        cursor.execute("INSERT INTO active_chats VALUES(%s,%s)",(uid,partner))
        cursor.execute("INSERT INTO active_chats VALUES(%s,%s)",(partner,uid))

        await context.bot.send_message(uid,"✅ Connected!")
        await context.bot.send_message(partner,"✅ Connected!")

    else:

        cursor.execute("INSERT INTO waiting_users VALUES(%s,%s)",(uid,pref))

        await update.message.reply_text("🔎 Searching for a partner...")

# ================= STOP =================

async def stop_chat(update,context):

    uid=update.message.from_user.id

    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s",(uid,))

    partner=get_partner(uid)

    if not partner:
        await update.message.reply_text("⛔ Search stopped",reply_markup=user_keyboard)
        return

    cursor.execute("DELETE FROM active_chats WHERE user_id=%s",(uid,))
    cursor.execute("DELETE FROM active_chats WHERE user_id=%s",(partner,))

    await update.message.reply_text("❌ Chat ended",reply_markup=user_keyboard)

    try:
        await context.bot.send_message(partner,"Stranger left the chat")
    except:
        pass

# ================= ROUTER =================

async def router(update:Update,context:ContextTypes.DEFAULT_TYPE):

    if not update.message:
        return

    uid=update.message.from_user.id
    text=update.message.text or ""

    # -------- REGISTRATION --------

    step=context.user_data.get("step")

    if step=="name":

        context.user_data["name"]=text
        context.user_data["step"]="gender"

        await update.message.reply_text("Select your gender",reply_markup=gender_keyboard)
        return

    if step=="gender":

        if text not in ["Male","Female"]:
            await update.message.reply_text("Please select gender using buttons")
            return

        context.user_data["gender"]=text
        context.user_data["step"]="country"

        await update.message.reply_text("Enter your country")
        return

    if step=="country":

        context.user_data["country"]=text
        context.user_data["step"]="age"

        await update.message.reply_text("Enter your age")
        return

    if step=="age":

        if not text.isdigit():
            await update.message.reply_text("Age must be a number")
            return

        cursor.execute("""
        UPDATE users
        SET name=%s,gender=%s,country=%s,age=%s
        WHERE user_id=%s
        """,(
        context.user_data["name"],
        context.user_data["gender"],
        context.user_data["country"],
        int(text),
        uid
        ))

        context.user_data["step"]=None

        await update.message.reply_text("Registration complete 🎉",reply_markup=user_keyboard)
        return

    # -------- USER BUTTONS --------

    if text=="🚀 Find Partner":
        await match_user(update,context)

    elif text=="👨 Find Male":

        if is_vip(uid):
            await match_user(update,context,"Male")
        else:
            await update.message.reply_text("👑 VIP required")

    elif text=="👩 Find Female":

        if is_vip(uid):
            await match_user(update,context,"Female")
        else:
            await update.message.reply_text("👑 VIP required")

    elif text=="⏭ Next":
        await stop_chat(update,context)
        await match_user(update,context)

    elif text=="❌ Stop":
        await stop_chat(update,context)

    elif text=="💎 VIP":
        await update.message.reply_text("VIP Menu",reply_markup=vip_keyboard)

    # -------- BACK BUTTON --------

    if text=="⬅ Back":
        await update.message.reply_text("User Menu",reply_markup=user_keyboard)
        return

# ================= START =================

async def start(update:Update,context:ContextTypes.DEFAULT_TYPE):

    uid=update.message.from_user.id
    username=update.message.from_user.username

    ref=int(context.args[0]) if context.args and context.args[0].isdigit() else None

    if user_exists(uid):

        cursor.execute("""
        SELECT name,gender,country,age
        FROM users
        WHERE user_id=%s
        """,(uid,))

        row=cursor.fetchone()

        if None in row:
            context.user_data["step"]="name"
            await update.message.reply_text("Enter your name")
            return

        await update.message.reply_text("Welcome back!",reply_markup=user_keyboard)
        return

    cursor.execute(
    "INSERT INTO users (user_id,username,referred_by) VALUES (%s,%s,%s)",
    (uid,username,ref)
    )

    context.user_data["step"]="name"

    await update.message.reply_text("Enter your name")

# ================= RUN =================

app=ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start",start))
app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND,router))

app.run_polling(drop_pending_updates=True)
