import os
import psycopg2
import asyncio
from datetime import datetime, timedelta
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

# --- CONFIG ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = 643086953

# Stable Connection with Keep-Alives
conn = psycopg2.connect(
    DATABASE_URL,
    keepalives=1,
    keepalives_idle=30,
    keepalives_interval=10,
    keepalives_count=5
)
conn.autocommit = True
cursor = conn.cursor()

# --- DATABASE SETUP ---
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
cursor.execute("CREATE TABLE IF NOT EXISTS waiting_users (user_id BIGINT PRIMARY KEY, preferred_gender TEXT)")
cursor.execute("CREATE TABLE IF NOT EXISTS active_chats (user_id BIGINT PRIMARY KEY, partner_id BIGINT)")

# --- KEYBOARDS ---
user_keyboard = ReplyKeyboardMarkup([
    ["🚀 Find Partner"],
    ["👨 Find Male", "👩 Find Female"],
    ["⏭ Next", "❌ Stop"],
    ["💎 VIP"]
], resize_keyboard=True)

vip_keyboard = ReplyKeyboardMarkup([
    ["🎁 Get FREE VIP"],
    ["👑 Contact Admin"],
    ["⬅ Back"]
], resize_keyboard=True)

gender_keyboard = ReplyKeyboardMarkup([["Male", "Female"]], resize_keyboard=True, one_time_keyboard=True)

# --- HELPERS ---
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
    if not row: return False
    vip, expiry = row
    return True if vip or (expiry and expiry > datetime.utcnow()) else False

# --- MATCHMAKING ---
async def match_user(update, context, pref=None):
    uid = update.message.from_user.id
    if get_partner(uid): return
    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (uid,))
    
    sql = "SELECT w.user_id FROM waiting_users w JOIN users u ON w.user_id=u.user_id WHERE u.gender=%s AND w.user_id!=%s LIMIT 1" if pref else "SELECT user_id FROM waiting_users WHERE user_id!=%s LIMIT 1"
    cursor.execute(sql, (pref, uid) if pref else (uid,))
    row = cursor.fetchone()

    if row:
        partner = row[0]
        cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (partner,))
        cursor.execute("INSERT INTO active_chats VALUES(%s,%s), (%s,%s)", (uid, partner, partner, uid))
        await context.bot.send_message(uid, "✅ Connected!")
        await context.bot.send_message(partner, "✅ Connected!")
    else:
        cursor.execute("INSERT INTO waiting_users (user_id, preferred_gender) VALUES (%s,%s) ON CONFLICT (user_id) DO NOTHING", (uid, pref))
        await update.message.reply_text("🔎 Searching...")

async def stop_chat(update, context):
    uid = update.message.from_user.id
    partner = get_partner(uid)
    if not partner: return
    cursor.execute("DELETE FROM active_chats WHERE user_id IN (%s, %s)", (uid, partner))
    await context.bot.send_message(uid, "❌ Chat ended.", reply_markup=user_keyboard)
    try: await context.bot.send_message(partner, "Stranger left.", reply_markup=user_keyboard)
    except: pass

# --- ROUTER ---
async def router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text: return
    uid, text = update.message.from_user.id, update.message.text

    if text in ["⬅ Back", "Back"]:
        context.user_data.clear()
        await update.message.reply_text("Main Menu", reply_markup=user_keyboard)
        return

    # REGISTRATION
    if context.user_data.get("step"):
        step = context.user_data["step"]
        if step == "name":
            context.user_data.update({"name": text, "step": "gender"})
            await update.message.reply_text("Select gender:", reply_markup=gender_keyboard)
        elif step == "gender":
            gen = text.capitalize()
            if gen not in ["Male", "Female"]:
                await update.message.reply_text("Use buttons.", reply_markup=gender_keyboard)
                return
            context.user_data.update({"gender": gen, "step": "country"})
            await update.message.reply_text("Enter country:", reply_markup=ReplyKeyboardRemove())
        elif step == "country":
            ref = context.user_data.get("ref")
            cursor.execute("INSERT INTO users(user_id,username,name,gender,country,referred_by) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (uid, update.message.from_user.username, context.user_data["name"], context.user_data["gender"], text, ref))
            context.user_data.clear()
            await update.message.reply_text("✅ Registration complete!", reply_markup=user_keyboard)
        return

    # USER BUTTONS
    if text == "🚀 Find Partner": await match_user(update, context)
    elif text == "👨 Find Male": 
        if is_vip(uid): await match_user(update, context, "Male")
        else: await update.message.reply_text("👑 VIP Required.")
    elif text == "👩 Find Female":
        if is_vip(uid): await match_user(update, context, "Female")
        else: await update.message.reply_text("👑 VIP Required.")
    elif text == "⏭ Next":
        await stop_chat(update, context)
        await match_user(update, context)
    elif text == "❌ Stop": await stop_chat(update, context)
    elif text == "💎 VIP": await update.message.reply_text("VIP Menu", reply_markup=vip_keyboard)
    elif text == "🎁 Get FREE VIP":
        await update.message.reply_text(f"Link: https://t.me/{context.bot.username}?start={uid}")
    elif text == "👑 Contact Admin":
        await update.message.reply_text("Contact Admin: @Random1204")

    # CHAT FORWARDING
    partner = get_partner(uid)
    if partner:
        try: await update.message.copy(chat_id=partner)
        except:
            cursor.execute("DELETE FROM active_chats WHERE user_id IN (%s, %s)", (uid, partner))
            await update.message.reply_text("Partner left. Searching...")
            await match_user(update, context)

# --- RUN ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.message.from_user.id
    if user_exists(uid):
        await update.message.reply_text("Welcome!", reply_markup=user_keyboard)
        return
    ref = int(context.args[0]) if context.args and context.args[0].isdigit() else None
    context.user_data["step"], context.user_data["ref"] = "name", ref
    await update.message.reply_text("Enter name:")

app = ApplicationBuilder().token(BOT_TOKEN).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, router))
app.run_polling(drop_pending_updates=True)

