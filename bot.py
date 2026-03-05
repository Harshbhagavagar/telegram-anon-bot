import os
import psycopg2
import asyncio
from datetime import datetime, timedelta
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = 643086953

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

user_keyboard=ReplyKeyboardMarkup(
[
["🚀 Find Partner"],
["👨 Find Male","👩 Find Female"],
["⏭ Next","❌ Stop"],
["💎 VIP"]
],
resize_keyboard=True
)

vip_keyboard=ReplyKeyboardMarkup(
[
["🎁 Get FREE VIP"],
["👑 Contact Admin"],
["⬅ Back"]
],
resize_keyboard=True
)

admin_keyboard=ReplyKeyboardMarkup(
[
["📊 Analytics"],
["📢 Announcement"],
["👥 Active Users","🕒 Waiting Users"],
["⬅ Back"]
],
resize_keyboard=True
)

gender_keyboard=ReplyKeyboardMarkup(
[
["Male","Female"]
],
resize_keyboard=True,
one_time_keyboard=True
)

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
    return vip or (expiry and expiry>datetime.utcnow())

async def broadcast(update:Update,context:ContextTypes.DEFAULT_TYPE):

    if update.message.from_user.id!=ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("Usage: /broadcast message")
        return

    msg=" ".join(context.args)

    cursor.execute("SELECT user_id FROM users")
    users=cursor.fetchall()

    sent=0

    for u in users:
        try:
            await context.bot.send_message(u[0],msg)
            sent+=1
            await asyncio.sleep(0.05)
        except:
            pass

    await update.message.reply_text(f"Sent to {sent} users")

async def match_user(update,context,pref=None):

    uid=update.message.from_user.id

    if get_partner(uid):
        return

    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s",(uid,))

    if pref:
        cursor.execute("""
        SELECT w.user_id
        FROM waiting_users w
        JOIN users u ON w.user_id=u.user_id
        WHERE u.gender=%s
        AND w.user_id!=%s
        LIMIT 1
        """,(pref,uid))
    else:
        cursor.execute("SELECT user_id FROM waiting_users WHERE user_id!=%s LIMIT 1",(uid,))

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

        async def invite_prompt():

            await asyncio.sleep(45)

            cursor.execute("SELECT 1 FROM waiting_users WHERE user_id=%s",(uid,))
            still_waiting=cursor.fetchone()

            if still_waiting:

                link=f"https://t.me/{context.bot.username}?start={uid}"

                await context.bot.send_message(
                uid,
f"""🔎 Still searching?

Invite 3 friends and unlock 👑 VIP for 3 days!

Your invite link:
{link}"""
                )

        asyncio.create_task(invite_prompt())

async def stop_chat(update,context):

    uid=update.message.from_user.id

    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s",(uid,))

    partner=get_partner(uid)

    if not partner:

        await update.message.reply_text("Search stopped",reply_markup=user_keyboard)
        return

    cursor.execute("DELETE FROM active_chats WHERE user_id=%s",(uid,))
    cursor.execute("DELETE FROM active_chats WHERE user_id=%s",(partner,))

    await update.message.reply_text("❌ Chat ended",reply_markup=user_keyboard)

    try:
        await context.bot.send_message(partner,"Stranger left the chat")
    except:
        pass

async def router(update:Update,context:ContextTypes.DEFAULT_TYPE):

    if not update.message:
        return

    uid=update.message.from_user.id
    text=update.message.text or ""

    step=context.user_data.get("step")

    if step=="name":

        context.user_data["name"]=text
        context.user_data["step"]="gender"

        await update.message.reply_text(
        "Select your gender",
        reply_markup=gender_keyboard
        )
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

        await update.message.reply_text(
        "Registration complete 🎉",
        reply_markup=user_keyboard
        )
        return

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

    partner=get_partner(uid)

    if partner:
        try:
            await update.message.copy(chat_id=partner)
        except:
            pass

async def start(update:Update,context:ContextTypes.DEFAULT_TYPE):

    uid=update.message.from_user.id
    username=update.message.from_user.username

    ref=int(context.args[0]) if context.args and context.args[0].isdigit() else None

    if user_exists(uid):
        await update.message.reply_text("Welcome back!",reply_markup=user_keyboard)
        return

    cursor.execute(
    "INSERT INTO users (user_id,username,referred_by) VALUES (%s,%s,%s)",
    (uid,username,ref)
    )

    context.user_data["step"]="name"

    await update.message.reply_text("Enter your name")

app=ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start",start))
app.add_handler(CommandHandler("broadcast",broadcast))
app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND,router))

app.run_polling(drop_pending_updates=True)
