import os
import psycopg2
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

# ================= CONFIG =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = 643086953  # your telegram id

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not set")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set")

# ================= DATABASE =================
conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cursor = conn.cursor()

# ================= TABLES =================
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    username TEXT,
    name TEXT,
    gender TEXT,
    country TEXT,
    is_vip BOOLEAN DEFAULT FALSE,
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

cursor.execute("""
CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    sender_id BIGINT,
    receiver_id BIGINT,
    content_type TEXT,
    content TEXT,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# ================= KEYBOARDS =================
main_keyboard = ReplyKeyboardMarkup(
    [
        ["Find Partner"],
        ["Find Male", "Find Female"],
        ["Next", "Stop"]
    ],
    resize_keyboard=True
)

gender_keyboard = ReplyKeyboardMarkup(
    [["Male", "Female"]],
    resize_keyboard=True,
    one_time_keyboard=True
)

# ================= START =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user

    cursor.execute("SELECT * FROM users WHERE user_id=%s", (user.id,))
    existing = cursor.fetchone()

    if existing:
        await update.message.reply_text(
            "Welcome back!",
            reply_markup=main_keyboard
        )
        return

    context.user_data["step"] = "name"
    await update.message.reply_text("Enter your name:")

# ================= REGISTRATION =================
async def registration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    step = context.user_data.get("step")
    text = update.message.text
    user = update.message.from_user

    if step == "name":
        context.user_data["name"] = text
        context.user_data["step"] = "gender"
        await update.message.reply_text("Select your gender:", reply_markup=gender_keyboard)
        return

    if step == "gender":
        if text not in ["Male", "Female"]:
            return
        context.user_data["gender"] = text
        context.user_data["step"] = "country"
        await update.message.reply_text("Enter your country:", reply_markup=ReplyKeyboardRemove())
        return

    if step == "country":
        cursor.execute("""
        INSERT INTO users (user_id, username, name, gender, country)
        VALUES (%s, %s, %s, %s, %s)
        """, (
            user.id,
            user.username,
            context.user_data["name"],
            context.user_data["gender"],
            text
        ))
        context.user_data.clear()
        await update.message.reply_text("Registration complete!", reply_markup=main_keyboard)

# ================= MATCH FUNCTION =================
async def match_user(update, context, preferred_gender=None):
    user_id = update.message.from_user.id

    # Remove previous state
    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (user_id,))
    cursor.execute("DELETE FROM active_chats WHERE user_id=%s", (user_id,))

    if preferred_gender:
        cursor.execute("""
        SELECT w.user_id FROM waiting_users w
        JOIN users u ON w.user_id=u.user_id
        WHERE u.gender=%s AND w.user_id != %s
        LIMIT 1
        """, (preferred_gender, user_id))
    else:
        cursor.execute("""
        SELECT user_id FROM waiting_users
        WHERE user_id != %s
        LIMIT 1
        """, (user_id,))

    row = cursor.fetchone()

    if row:
        partner = row[0]
        cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (partner,))
        cursor.execute("INSERT INTO active_chats VALUES (%s, %s)", (user_id, partner))
        cursor.execute("INSERT INTO active_chats VALUES (%s, %s)", (partner, user_id))
        await context.bot.send_message(user_id, "Connected!")
        await context.bot.send_message(partner, "Connected!")
    else:
        cursor.execute("INSERT INTO waiting_users VALUES (%s, %s)", (user_id, preferred_gender))
        await update.message.reply_text("Searching for partner...")

# ================= STOP =================
async def stop_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    cursor.execute("SELECT partner_id FROM active_chats WHERE user_id=%s", (user_id,))
    row = cursor.fetchone()

    if not row:
        await update.message.reply_text("You are not connected.", reply_markup=main_keyboard)
        return

    partner = row[0]

    cursor.execute("DELETE FROM active_chats WHERE user_id=%s", (user_id,))
    cursor.execute("DELETE FROM active_chats WHERE user_id=%s", (partner,))

    await context.bot.send_message(user_id, "Chat ended.", reply_markup=main_keyboard)
    await context.bot.send_message(partner, "Stranger left.")

# ================= BUTTON HANDLER =================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    text = update.message.text

    if context.user_data.get("step"):
        await registration(update, context)
        return

    if text == "Find Partner":
        await match_user(update, context)
        return

    if text in ["Find Male", "Find Female"]:
        cursor.execute("SELECT is_vip FROM users WHERE user_id=%s", (user_id,))
        is_vip = cursor.fetchone()[0]

        if not is_vip:
            await update.message.reply_text("Please contact admin for VIP access.")
            return

        gender = "Male" if text == "Find Male" else "Female"
        await match_user(update, context, gender)
        return

    if text == "Next":
        await stop_chat(update, context)
        await match_user(update, context)
        return

    if text == "Stop":
        await stop_chat(update, context)
        return

# ================= CHAT =================
async def chat_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    cursor.execute("SELECT partner_id FROM active_chats WHERE user_id=%s", (user_id,))
    row = cursor.fetchone()

    if not row:
        return

    partner = row[0]

    content_type = "text"
    content = update.message.text or "media"

    cursor.execute("""
    INSERT INTO messages (sender_id, receiver_id, content_type, content)
    VALUES (%s, %s, %s, %s)
    """, (user_id, partner, content_type, content))

    cursor.execute("""
    UPDATE users SET total_messages=total_messages+1 WHERE user_id=%s
    """, (user_id,))

    await update.message.copy(chat_id=partner)

# ================= ADMIN =================
async def analytics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return

    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM active_chats")
    active = cursor.fetchone()[0] // 2

    await update.message.reply_text(
        f"Total Users: {total_users}\nActive Chats: {active}"
    )

# ================= RUN =================
app = ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("analytics", analytics))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, button_handler))
app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, chat_handler))

app.run_polling(drop_pending_updates=True)
