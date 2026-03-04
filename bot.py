import os
import psycopg2
from datetime import datetime, timedelta
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

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
["👨 Find Male","👩 Find Female"],
["⏭ Next","❌ Stop"],
["💎 VIP"]
],
resize_keyboard=True
)

vip_keyboard = ReplyKeyboardMarkup(
[
["🎁 Get FREE VIP"],
["👑 Contact Admin Id in Bio"],
["⬅ Back"]
],
resize_keyboard=True
)

admin_keyboard = ReplyKeyboardMarkup(
[
["📊 Analytics"],
["👥 Active Users","🕒 Waiting Users"],
["⬅ Back"]
],
resize_keyboard=True
)

gender_keyboard = ReplyKeyboardMarkup(
[["Male","Female"]],
resize_keyboard=True,
one_time_keyboard=True
)

# ================= HELPERS =================

def user_exists(user_id):
    cursor.execute("SELECT 1 FROM users WHERE user_id=%s",(user_id,))
    return cursor.fetchone() is not None

def get_partner(user_id):
    cursor.execute("SELECT partner_id FROM active_chats WHERE user_id=%s",(user_id,))
    return cursor.fetchone()

def is_vip(user_id):

    cursor.execute("SELECT is_vip,vip_expiry FROM users WHERE user_id=%s",(user_id,))
    row=cursor.fetchone()

    if not row:
        return False

    vip=row[0]
    expiry=row[1]

    if vip:
        return True

    if expiry and expiry>datetime.utcnow():
        return True

    return False

def reward_referral(user_id):

    cursor.execute("SELECT referral_count FROM users WHERE user_id=%s",(user_id,))
    count=cursor.fetchone()[0]

    if count>=3:

        expiry=datetime.utcnow()+timedelta(days=3)

        cursor.execute("""
        UPDATE users
        SET vip_expiry=%s
        WHERE user_id=%s
        """,(expiry,user_id))

# ================= START =================

async def start(update:Update,context:ContextTypes.DEFAULT_TYPE):

    user=update.message.from_user
    user_id=user.id

    ref=None
    if context.args:
        try:
            ref=int(context.args[0])
        except:
            pass

    if user_id==ADMIN_ID:
        await update.message.reply_text("👑 Admin Panel",reply_markup=admin_keyboard)
        return

    if user_exists(user_id):
        await update.message.reply_text("Welcome back!",reply_markup=user_keyboard)
        return

    context.user_data["step"]="name"
    context.user_data["ref"]=ref

    await update.message.reply_text("Enter your name:")

# ================= MATCH =================

async def match_user(update,context,preferred_gender=None):

    user_id=update.message.from_user.id

    if get_partner(user_id):
        await update.message.reply_text("⚠️ Already chatting.")
        return

    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s",(user_id,))

    if preferred_gender:

        cursor.execute("""
        SELECT w.user_id
        FROM waiting_users w
        JOIN users u ON w.user_id=u.user_id
        WHERE u.gender=%s
        AND w.user_id!=%s
        LIMIT 1
        """,(preferred_gender,user_id))

    else:

        cursor.execute("""
        SELECT user_id
        FROM waiting_users
        WHERE user_id!=%s
        LIMIT 1
        """,(user_id,))

    row=cursor.fetchone()

    if row:

        partner=row[0]

        cursor.execute("DELETE FROM waiting_users WHERE user_id=%s",(partner,))
        cursor.execute("INSERT INTO active_chats VALUES(%s,%s)",(user_id,partner))
        cursor.execute("INSERT INTO active_chats VALUES(%s,%s)",(partner,user_id))

        await context.bot.send_message(user_id,"✅ Connected!")
        await context.bot.send_message(partner,"✅ Connected!")

    else:

        cursor.execute("INSERT INTO waiting_users VALUES(%s,%s)",(user_id,preferred_gender))
        await update.message.reply_text("🔎 Searching...")

# ================= STOP =================

async def stop_chat(update,context):

    user_id=update.message.from_user.id

    row=get_partner(user_id)

    if not row:
        await update.message.reply_text("Not connected.",reply_markup=user_keyboard)
        return

    partner=row[0]

    cursor.execute("DELETE FROM active_chats WHERE user_id=%s",(user_id,))
    cursor.execute("DELETE FROM active_chats WHERE user_id=%s",(partner,))

    await context.bot.send_message(user_id,"❌ Chat ended.",reply_markup=user_keyboard)
    await context.bot.send_message(partner,"Stranger left.")

# ================= TEMP VIP PROMOTION =================
# DELETE THIS WHOLE SECTION AFTER PROMOTION ENDS

async def vip_offer(context: ContextTypes.DEFAULT_TYPE):

    cursor.execute("SELECT user_id FROM users")
    users = cursor.fetchall()

    for u in users:

        uid = u[0]

        try:

            link=f"https://t.me/{context.bot.username}?start={uid}"

            await context.bot.send_message(
                uid,
                f"""🔥 LIMITED VIP OFFER (24 HOURS)

Invite 3 friends and get

💎 FREE VIP for 3 days

VIP Benefits
👩 Gender Filter
⚡ Faster Matching

Your Invite Link:
{link}

⏳ Offer ending soon!
"""
            )

        except:
            pass

# ================= ROUTER =================

async def router(update:Update,context:ContextTypes.DEFAULT_TYPE):

    if not update.message:
        return

    user=update.message.from_user
    user_id=user.id
    text=update.message.text or ""

    if text=="💎 VIP":
        await update.message.reply_text("VIP Menu:",reply_markup=vip_keyboard)
        return

    if text=="🎁 Get FREE VIP":

        link=f"https://t.me/{context.bot.username}?start={user_id}"

        cursor.execute("SELECT referral_count FROM users WHERE user_id=%s",(user_id,))
        count=cursor.fetchone()[0]

        await update.message.reply_text(
        f"""Invite friends using this link

{link}

Progress: {count}/3

3 invites = FREE VIP"""
        )

        return

    if text=="⬅ Back":
        await update.message.reply_text("Menu",reply_markup=user_keyboard)
        return

    if "Find Partner" in text:
        await match_user(update,context)
        return

    if "Next" in text:
        if get_partner(user_id):
            await stop_chat(update,context)
        await match_user(update,context)
        return

    if "Stop" in text:
        await stop_chat(update,context)
        return

    partner_row=get_partner(user_id)

    if partner_row:

        partner=partner_row[0]

        await update.message.copy(chat_id=partner)

        cursor.execute("""
        UPDATE users
        SET total_messages=total_messages+1
        WHERE user_id=%s
        """,(user_id,))

# ================= RUN =================

app=ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start",start))
app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND,router))

# SEND PROMO EVERY 3 HOURS
app.job_queue.run_repeating(vip_offer, interval=10800, first=60)

app.run_polling(drop_pending_updates=True)
