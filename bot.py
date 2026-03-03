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
    total_messages INT DEFAULT 0,
    referred_by BIGINT,
    referral_count INT DEFAULT 0,
    vip_expiry TIMESTAMP,
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
        ["🎁 My Referral"]
    ],
    resize_keyboard=True
)

admin_keyboard = ReplyKeyboardMarkup(
    [
        ["📊 Analytics"],
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
def user_exists(user_id):
    cursor.execute("SELECT 1 FROM users WHERE user_id=%s", (user_id,))
    return cursor.fetchone() is not None

def get_partner(user_id):
    cursor.execute("SELECT partner_id FROM active_chats WHERE user_id=%s", (user_id,))
    return cursor.fetchone()

def is_vip(user_id):
    cursor.execute("SELECT is_vip, vip_expiry FROM users WHERE user_id=%s", (user_id,))
    row = cursor.fetchone()
    if not row:
        return False
    vip_flag, expiry = row
    if expiry and expiry > datetime.utcnow():
        return True
    return vip_flag

# ================= START =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user

    if user.id == ADMIN_ID:
        await update.message.reply_text("👑 Admin Panel", reply_markup=admin_keyboard)
        return

    if user_exists(user.id):
        await update.message.reply_text("Welcome back!", reply_markup=user_keyboard)
        return

    context.user_data["step"] = "name"
    await update.message.reply_text("Enter your name:")

# ================= MATCH =================
async def match_user(update, context, preferred_gender=None):
    user_id = update.message.from_user.id

    if get_partner(user_id):
        await update.message.reply_text("⚠️ Already in chat. Press Stop first.")
        return

    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (user_id,))

    cursor.execute("SELECT gender FROM users WHERE user_id=%s", (user_id,))
    row = cursor.fetchone()
    if not row:
        return
    my_gender = row[0]

    if preferred_gender:
        cursor.execute("""
            SELECT w.user_id
            FROM waiting_users w
            JOIN users u ON w.user_id=u.user_id
            WHERE u.gender=%s
              AND w.user_id!=%s
              AND (w.preferred_gender IS NULL OR w.preferred_gender=%s)
            LIMIT 1
        """, (preferred_gender, user_id, my_gender))
    else:
        cursor.execute("""
            SELECT w.user_id
            FROM waiting_users w
            JOIN users u ON w.user_id=u.user_id
            WHERE w.user_id!=%s
              AND (w.preferred_gender IS NULL OR w.preferred_gender=%s)
            LIMIT 1
        """, (user_id, my_gender))

    partner_row = cursor.fetchone()

    if partner_row:
        partner = partner_row[0]

        cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (partner,))
        cursor.execute("INSERT INTO active_chats VALUES (%s,%s) ON CONFLICT (user_id) DO UPDATE SET partner_id=EXCLUDED.partner_id", (user_id, partner))
        cursor.execute("INSERT INTO active_chats VALUES (%s,%s) ON CONFLICT (user_id) DO UPDATE SET partner_id=EXCLUDED.partner_id", (partner, user_id))

        await context.bot.send_message(user_id, "✅ Connected!")
        await context.bot.send_message(partner, "✅ Connected!")
    else:
        cursor.execute("INSERT INTO waiting_users VALUES (%s,%s) ON CONFLICT (user_id) DO UPDATE SET preferred_gender=EXCLUDED.preferred_gender", (user_id, preferred_gender))
        await update.message.reply_text("🔎 Searching...")

# ================= STOP =================
async def stop_chat(update, context):
    user_id = update.message.from_user.id
    row = get_partner(user_id)

    if not row:
        await update.message.reply_text("Not connected.", reply_markup=user_keyboard)
        return

    partner = row[0]
    cursor.execute("DELETE FROM active_chats WHERE user_id=%s", (user_id,))
    cursor.execute("DELETE FROM active_chats WHERE user_id=%s", (partner,))

    await context.bot.send_message(user_id, "❌ Chat ended.", reply_markup=user_keyboard)
    await context.bot.send_message(partner, "Stranger left.")

# ================= ROUTER =================
async def router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    user_id = user.id
    text = update.message.text

    # ===== ADMIN PANEL =====
    if user_id == ADMIN_ID:
        if text == "📊 Analytics":
            cursor.execute("SELECT COUNT(*) FROM users")
            total = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM active_chats")
            active = cursor.fetchone()[0] // 2
            await update.message.reply_text(f"👥 Users: {total}\n💬 Active Chats: {active}")
            return

        if text == "👥 Active Users":
            cursor.execute("SELECT COUNT(*) FROM active_chats")
            active = cursor.fetchone()[0] // 2
            await update.message.reply_text(f"Active Chats: {active}")
            return

        if text == "🕒 Waiting Users":
            cursor.execute("SELECT COUNT(*) FROM waiting_users")
            waiting = cursor.fetchone()[0]
            await update.message.reply_text(f"Waiting: {waiting}")
            return

        if text == "⬅ Back":
            await update.message.reply_text("Back to user menu.", reply_markup=user_keyboard)
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
            context.user_data["gender"] = text
            context.user_data["step"] = "country"
            await update.message.reply_text("Enter country:", reply_markup=ReplyKeyboardRemove())
            return

        if step == "country":
            cursor.execute("""
                INSERT INTO users (user_id, username, name, gender, country)
                VALUES (%s,%s,%s,%s,%s)
            """, (user_id, user.username, context.user_data["name"], context.user_data["gender"], text))
            context.user_data.clear()
            await update.message.reply_text("✅ Registration complete!", reply_markup=user_keyboard)
            return

    if not user_exists(user_id):
        await update.message.reply_text("Use /start first.")
        return

    # ===== BUTTONS =====
    if text == "🚀 Find Partner":
        await match_user(update, context)
        return

    if text in ("👨 Find Male", "👩 Find Female"):
        if not is_vip(user_id):
            await update.message.reply_text("👑 VIP required for gender search.")
            return
        gender = "Male" if "Male" in text else "Female"
        await match_user(update, context, gender)
        return

    if text == "⏭ Next":
        if get_partner(user_id):
            await stop_chat(update, context)
        await match_user(update, context)
        return

    if text == "❌ Stop":
        await stop_chat(update, context)
        return

    # ===== CHAT FORWARD =====
    partner_row = get_partner(user_id)
    if partner_row:
        partner = partner_row[0]
        cursor.execute("UPDATE users SET total_messages=total_messages+1 WHERE user_id=%s", (user_id,))
        await update.message.copy(chat_id=partner)
        return

# ================= RUN =================
app = ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, router))

app.run_polling(drop_pending_updates=True)
