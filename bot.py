import os
import psycopg2
import asyncio
from datetime import datetime, timedelta
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    InlineKeyboardButton,
    InlineKeyboardMarkup
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters
)

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

def user_exists(uid):
    cursor.execute("SELECT 1 FROM users WHERE user_id=%s",(uid,))
    return cursor.fetchone() is not None

def get_partner(uid):
    cursor.execute("SELECT partner_id FROM active_chats WHERE user_id=%s",(uid,))
    row = cursor.fetchone()
    return row[0] if row else None

def is_vip(uid):
    cursor.execute("SELECT is_vip,vip_expiry FROM users WHERE user_id=%s",(uid,))
    row = cursor.fetchone()

    if not row:
        return False

    vip,expiry=row

    if vip:
        return True

    if expiry and expiry>datetime.utcnow():
        return True

    return False

def reward_referral(uid):

    cursor.execute("SELECT referral_count FROM users WHERE user_id=%s",(uid,))
    row=cursor.fetchone()

    if not row:
        return

    if row[0] >= 3:

        expiry=datetime.utcnow()+timedelta(days=3)

        cursor.execute("""
        UPDATE users
        SET vip_expiry=%s
        WHERE user_id=%s
        """,(expiry,uid))

# ================= ADMIN COMMANDS =================

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if update.message.from_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("Use: /broadcast message")
        return

    msg=" ".join(context.args)

    cursor.execute("SELECT user_id FROM users")
    rows=cursor.fetchall()

    sent=0

    for r in rows:
        try:
            await context.bot.send_message(r[0],msg)
            sent+=1
            await asyncio.sleep(0.05)
        except:
            continue

    await update.message.reply_text(f"Broadcast sent to {sent} users")

async def find_user(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if update.message.from_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("Use: /find name or id")
        return

    q=context.args[0]

    if q.isdigit():
        cursor.execute("SELECT user_id,name,is_vip FROM users WHERE user_id=%s",(int(q),))
    else:
        cursor.execute("SELECT user_id,name,is_vip FROM users WHERE name ILIKE %s",(f"%{q}%",))

    rows=cursor.fetchall()

    if not rows:
        await update.message.reply_text("User not found")
        return

    for uid,name,vip in rows:

        status="VIP" if vip else "Normal"

        text=f"Name: {name}\nID: {uid}\nStatus: {status}"

        if vip:
            btn=InlineKeyboardButton("Remove VIP",callback_data=f"unvip_{uid}")
        else:
            btn=InlineKeyboardButton("Make VIP",callback_data=f"vip_{uid}")

        keyboard=InlineKeyboardMarkup([[btn]])

        await update.message.reply_text(text,reply_markup=keyboard)

async def vip_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):

    query=update.callback_query
    await query.answer()

    data=query.data
    uid=int(data.split("_")[1])

    if data.startswith("vip_"):
        cursor.execute("UPDATE users SET is_vip=TRUE WHERE user_id=%s",(uid,))
        await query.edit_message_text("User promoted to VIP")

    else:
        cursor.execute("UPDATE users SET is_vip=FALSE WHERE user_id=%s",(uid,))
        await query.edit_message_text("VIP removed")

# ================= START =================

async def start(update:Update,context:ContextTypes.DEFAULT_TYPE):

    user=update.message.from_user
    uid=user.id

    ref=None

    if context.args:
        try:
            ref=int(context.args[0])
        except:
            pass

    if uid==ADMIN_ID:
        await update.message.reply_text("Admin panel",reply_markup=admin_keyboard)
        return

    if user_exists(uid):
        await update.message.reply_text("Welcome back",reply_markup=user_keyboard)
        return

    context.user_data["step"]="name"
    context.user_data["ref"]=ref

    await update.message.reply_text("Enter your name")

# ================= MATCH =================

async def match_user(update,context,preferred_gender=None):

    uid=update.message.from_user.id

    if get_partner(uid):
        await update.message.reply_text("Already chatting")
        return

    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s",(uid,))

    if preferred_gender:

        cursor.execute("""
        SELECT w.user_id
        FROM waiting_users w
        JOIN users u ON w.user_id=u.user_id
        WHERE u.gender=%s
        AND w.user_id!=%s
        LIMIT 1
        """,(preferred_gender,uid))

    else:

        cursor.execute("""
        SELECT user_id FROM waiting_users
        WHERE user_id!=%s
        LIMIT 1
        """,(uid,))

    row=cursor.fetchone()

    if row:

        partner=row[0]

        cursor.execute("DELETE FROM waiting_users WHERE user_id=%s",(partner,))
        cursor.execute("INSERT INTO active_chats VALUES(%s,%s)",(uid,partner))
        cursor.execute("INSERT INTO active_chats VALUES(%s,%s)",(partner,uid))

        await context.bot.send_message(uid,"Connected")
        await context.bot.send_message(partner,"Connected")

    else:

        cursor.execute("INSERT INTO waiting_users VALUES(%s,%s)",(uid,preferred_gender))
        await update.message.reply_text("Searching...")

# ================= STOP =================

async def stop_chat(update,context):

    uid=update.message.from_user.id
    partner=get_partner(uid)

    if not partner:
        await update.message.reply_text("Not connected",reply_markup=user_keyboard)
        return

    cursor.execute("DELETE FROM active_chats WHERE user_id=%s",(uid,))
    cursor.execute("DELETE FROM active_chats WHERE user_id=%s",(partner,))

    try:
        await context.bot.send_message(uid,"Chat ended",reply_markup=user_keyboard)
        await context.bot.send_message(partner,"Stranger left")
    except:
        pass

# ================= ROUTER =================

async def router(update:Update,context:ContextTypes.DEFAULT_TYPE):

    if not update.message:
        return

    user = update.message.from_user
    uid = user.id
    text = update.message.text or ""

# ===== ADMIN PANEL =====

    if uid == ADMIN_ID:

        if text == "📊 Analytics":

            cursor.execute("SELECT COUNT(*) FROM users")
            total = cursor.fetchone()[0]

            cursor.execute("SELECT gender, COUNT(*) FROM users GROUP BY gender")
            genders = dict(cursor.fetchall())

            cursor.execute("SELECT COUNT(*) FROM active_chats")
            active = cursor.fetchone()[0] // 2

            msg = (
                f"BOT ANALYTICS\n\n"
                f"Total Users: {total}\n"
                f"Active Chats: {active}\n\n"
                f"Male: {genders.get('Male',0)}\n"
                f"Female: {genders.get('Female',0)}"
            )

            await update.message.reply_text(msg)
            return

        if text=="👥 Active Users":
            cursor.execute("SELECT COUNT(*) FROM active_chats")
            active=cursor.fetchone()[0]//2
            await update.message.reply_text(f"Active chats: {active}")
            return

        if text=="🕒 Waiting Users":
            cursor.execute("SELECT COUNT(*) FROM waiting_users")
            waiting=cursor.fetchone()[0]
            await update.message.reply_text(f"Waiting users: {waiting}")
            return

# ===== REGISTRATION =====

    if context.user_data.get("step"):

        step = context.user_data["step"]

        if step == "name":
            context.user_data["name"] = text
            context.user_data["step"] = "gender"
            await update.message.reply_text("Select gender:", reply_markup=gender_keyboard)
            return

        if step == "gender":

            formatted_gender = text.capitalize()

            if formatted_gender not in ["Male","Female"]:
                await update.message.reply_text("Use buttons", reply_markup=gender_keyboard)
                return

            context.user_data["gender"]=formatted_gender
            context.user_data["step"]="country"

            await update.message.reply_text("Enter country",reply_markup=ReplyKeyboardRemove())
            return

        if step=="country":

            ref=context.user_data.get("ref")

            cursor.execute("""
            INSERT INTO users(user_id,username,name,gender,country,referred_by)
            VALUES(%s,%s,%s,%s,%s,%s)
            ON CONFLICT (user_id) DO NOTHING
            """,(uid,user.username,context.user_data["name"],context.user_data["gender"],text,ref))

            if ref:
                cursor.execute("UPDATE users SET referral_count=referral_count+1 WHERE user_id=%s",(ref,))
                reward_referral(ref)

            context.user_data.clear()

            await update.message.reply_text("Registration complete",reply_markup=user_keyboard)
            return

# ===== USER BUTTONS =====

    if "Find Partner" in text:
        await match_user(update,context)
        return

    if "Find Male" in text:

        if not is_vip(uid):
            await update.message.reply_text("VIP required")
            return

        await match_user(update,context,"Male")
        return

    if "Find Female" in text:

        if not is_vip(uid):
            await update.message.reply_text("VIP required")
            return

        await match_user(update,context,"Female")
        return

    if "Next" in text:

        if get_partner(uid):
            await stop_chat(update,context)

        await match_user(update,context)
        return

    if "Stop" in text:
        await stop_chat(update,context)
        return

# ===== CHAT FORWARD =====

    partner=get_partner(uid)

    if partner:

        try:
            await update.message.copy(chat_id=partner)

        except:

            cursor.execute("DELETE FROM active_chats WHERE user_id=%s",(uid,))
            cursor.execute("DELETE FROM active_chats WHERE user_id=%s",(partner,))

            await update.message.reply_text("Partner disconnected. Searching new partner")
            await match_user(update,context)

# ================= RUN =================

app=ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start",start))
app.add_handler(CommandHandler("broadcast",broadcast))
app.add_handler(CommandHandler("find",find_user))
app.add_handler(CallbackQueryHandler(vip_toggle))
app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND,router))

app.run_polling(drop_pending_updates=True)
