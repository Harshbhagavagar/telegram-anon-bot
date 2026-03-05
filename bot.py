import os
import psycopg2
from datetime import datetime, timedelta
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

================= CONFIG =================

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = 643086953

if not BOT_TOKEN:
raise ValueError("BOT_TOKEN not set")

if not DATABASE_URL:
raise ValueError("DATABASE_URL not set")

================= DATABASE =================

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

================= KEYBOARDS =================

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

================= HELPERS =================

def user_exists(user_id):
cursor.execute("SELECT 1 FROM users WHERE user_id=%s",(user_id,))
return cursor.fetchone() is not None

def get_partner(user_id):
cursor.execute("SELECT partner_id FROM active_chats WHERE user_id=%s",(user_id,))
row = cursor.fetchone()
return row[0] if row else None

def is_vip(user_id):

cursor.execute("SELECT is_vip,vip_expiry FROM users WHERE user_id=%s",(user_id,))  
row = cursor.fetchone()  

if not row:  
    return False  

vip, expiry = row  

if vip:  
    return True  

if expiry and expiry > datetime.utcnow():  
    return True  

return False

def reward_referral(user_id):

cursor.execute("SELECT referral_count FROM users WHERE user_id=%s",(user_id,))  
row = cursor.fetchone()  

if not row:  
    return  

count = row[0]  

if count >= 3:  

    expiry = datetime.utcnow() + timedelta(days=3)  

    cursor.execute("""  
    UPDATE users  
    SET vip_expiry=%s  
    WHERE user_id=%s  
    """,(expiry,user_id))

================= START =================

async def start(update:Update,context:ContextTypes.DEFAULT_TYPE):

user = update.message.from_user  
user_id = user.id  
username = user.username if user.username else ""  

ref = None  

if context.args:  
    try:  
        ref = int(context.args[0])  
    except:  
        pass  

if user_id == ADMIN_ID:  
    await update.message.reply_text("👑 Admin Panel",reply_markup=admin_keyboard)  
    return  

if user_exists(user_id):  
    await update.message.reply_text("Welcome back!",reply_markup=user_keyboard)  
    return  

context.user_data["step"]="name"  
context.user_data["ref"]=ref  

await update.message.reply_text("Enter your name:")

================= MATCH =================

async def match_user(update,context,preferred_gender=None):

user_id = update.message.from_user.id  

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

row = cursor.fetchone()  

if row:  

    partner = row[0]  

    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s",(partner,))  
    cursor.execute("INSERT INTO active_chats VALUES(%s,%s)",(user_id,partner))  
    cursor.execute("INSERT INTO active_chats VALUES(%s,%s)",(partner,user_id))  

    try:  
        await context.bot.send_message(user_id,"✅ Connected!")  
    except:  
        pass  

    try:  
        await context.bot.send_message(partner,"✅ Connected!")  
    except:  
        pass  

else:  

    cursor.execute("INSERT INTO waiting_users VALUES(%s,%s)",(user_id,preferred_gender))  
    await update.message.reply_text("🔎 Searching...")

================= STOP =================

async def stop_chat(update,context):

user_id = update.message.from_user.id  
partner = get_partner(user_id)  

if not partner:  
    await update.message.reply_text("Not connected.",reply_markup=user_keyboard)  
    return  

cursor.execute("DELETE FROM active_chats WHERE user_id=%s",(user_id,))  
cursor.execute("DELETE FROM active_chats WHERE user_id=%s",(partner,))  

try:  
    await context.bot.send_message(user_id,"❌ Chat ended.",reply_markup=user_keyboard)  
except:  
    pass  

try:  
    await context.bot.send_message(partner,"Stranger left.")  
except:  
    pass

================= ROUTER =================

async def router(update:Update,context:ContextTypes.DEFAULT_TYPE):

if not update.message:  
    return  

user = update.message.from_user  
user_id = user.id  
text = update.message.text or ""

===== ADMIN =====

if user_id == ADMIN_ID:  

    if text=="📊 Analytics":  

        cursor.execute("SELECT COUNT(*) FROM users")  
        total = cursor.fetchone()[0]  

        cursor.execute("SELECT COUNT(*) FROM active_chats")  
        active = cursor.fetchone()[0]//2  

        await update.message.reply_text(  
        f"👥 Total Users: {total}\n💬 Active Chats: {active}")  
        return  

    if text=="👥 Active Users":  

        cursor.execute("SELECT COUNT(*) FROM active_chats")  
        active = cursor.fetchone()[0]//2  

        await update.message.reply_text(f"Active Chats: {active}")  
        return  

    if text=="🕒 Waiting Users":  

        cursor.execute("SELECT COUNT(*) FROM waiting_users")  
        waiting = cursor.fetchone()[0]  

        await update.message.reply_text(f"Waiting Users: {waiting}")  
        return  

    if text=="⬅ Back":  
        await update.message.reply_text("Back",reply_markup=user_keyboard)  
        return

===== REGISTRATION =====

if context.user_data.get("step"):  

    if not text:  
        await update.message.reply_text("⚠️ Please send text only during registration.")  
        return  

    step=context.user_data["step"]  

    if step=="name":  

        context.user_data["name"]=text  
        context.user_data["step"]="gender"  

        await update.message.reply_text(  
        "Select gender:",reply_markup=gender_keyboard)  
        return  

    if step=="gender":  

        if text not in ["Male","Female"]:  
            await update.message.reply_text(  
            "⚠️ Please use the buttons.",  
            reply_markup=gender_keyboard)  
            return  

        context.user_data["gender"]=text  
        context.user_data["step"]="country"  

        await update.message.reply_text(  
        "Enter country:",  
        reply_markup=ReplyKeyboardRemove())  
        return  

    if step=="country":  

        ref=context.user_data.get("ref")  

        cursor.execute("""  
        INSERT INTO users(user_id,username,name,gender,country,referred_by)  
        VALUES(%s,%s,%s,%s,%s,%s)  
        """,(user_id,user.username,  
        context.user_data["name"],  
        context.user_data["gender"],  
        text,ref))  

        if ref:  

            cursor.execute("""  
            UPDATE users  
            SET referral_count=referral_count+1  
            WHERE user_id=%s  
            """,(ref,))  

            reward_referral(ref)  

        context.user_data.clear()  

        await update.message.reply_text(  
        "✅ Registration complete!",  
        reply_markup=user_keyboard)  
        return

===== VIP MENU =====

if text=="💎 VIP":  
    await update.message.reply_text(  
    "VIP Menu:",reply_markup=vip_keyboard)  
    return  


if text=="🎁 Get FREE VIP":  

    link=f"https://t.me/{context.bot.username}?start={user_id}"  

    cursor.execute("SELECT referral_count FROM users WHERE user_id=%s",(user_id,))  
    row=cursor.fetchone()  

    count=row[0] if row else 0  

    await update.message.reply_text(  
    f"Invite friends:\n{link}\n\nProgress: {count}/3\n3 invites = 3 days VIP")  
    return  


if text=="👑 Contact Admin":  
    await update.message.reply_text(  
    "Contact admin for VIP purchase.")  
    return  


if text=="⬅ Back":  
    await update.message.reply_text(  
    "Menu",reply_markup=user_keyboard)  
    return

===== BUTTONS =====

if "Find Partner" in text:  
    await match_user(update,context)  
    return  


if "Find Male" in text:  

    if not is_vip(user_id):  
        await update.message.reply_text("👑 VIP required.")  
        return  

    await match_user(update,context,"Male")  
    return  


if "Find Female" in text:  

    if not is_vip(user_id):  
        await update.message.reply_text("👑 VIP required.")  
        return  

    await match_user(update,context,"Female")  
    return  


if "Next" in text:  

    if get_partner(user_id):  
        await stop_chat(update,context)  

    await match_user(update,context)  
    return  


if "Stop" in text:  
    await stop_chat(update,context)  
    return

===== CHAT FORWARD =====

partner = get_partner(user_id)  

if partner:  

    try:  
        await update.message.copy(chat_id=partner)  

    except Exception as e:  

        # remove broken chat from database  
        cursor.execute("DELETE FROM active_chats WHERE user_id=%s",(user_id,))  
        cursor.execute("DELETE FROM active_chats WHERE user_id=%s",(partner,))  

        await update.message.reply_text(  
            "⚠️ Partner disconnected. Searching new partner..."  
        )  

        # automatically search new partner  
        await match_user(update,context)  
        return  

    cursor.execute("""  
    UPDATE users  
    SET total_messages=total_messages+1  
    WHERE user_id=%s  
    """,(user_id,))  

    return

================= RUN =================

app=ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start",start))
app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND,router))

app.run_polling(drop_pending_updates=True)
