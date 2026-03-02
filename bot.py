import os
import psycopg2
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

# ================= ENV =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

ADMIN_ID = 643086953  # YOUR TELEGRAM ID

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not set")

# ================= DATABASE =================
conn = None
cursor = None

try:
    if not DATABASE_URL:
        print("WARNING: DATABASE_URL not found")
    else:
        print("DATABASE_URL loaded")

        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
        cursor = conn.cursor()

        print("Database connected successfully")

except Exception as e:
    print("Database connection failed:", e)

# ================= CREATE TABLES =================
if cursor:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT PRIMARY KEY,
        username TEXT,
        name TEXT,
        age TEXT,
        gender TEXT,
        country TEXT,
        total_messages INT DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        id SERIAL PRIMARY KEY,
        sender_id BIGINT,
        receiver_id BIGINT,
        content TEXT,
        sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

# ================= MEMORY =================
waiting_users = []
active_chats = {}

# ================= KEYBOARDS =================
main_keyboard = ReplyKeyboardMarkup(
    [["/find", "/stop"]],
    resize_keyboard=True
)

gender_keyboard = ReplyKeyboardMarkup(
    [["👨 Male", "👩 Female"]],
    resize_keyboard=True,
    one_time_keyboard=True
)

# ================= START =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    user_id = user.id

    if cursor:
        cursor.execute("SELECT * FROM users WHERE user_id=%s", (user_id,))
        existing = cursor.fetchone()
    else:
        existing = None

    if existing:
        await update.message.reply_text(
            "Welcome back!\nUse /find to connect.",
            reply_markup=main_keyboard
        )
        return

    context.user_data["step"] = "name"
    context.user_data["username"] = user.username

    await update.message.reply_text("Welcome! What is your name?")

# ================= REGISTRATION =================
async def registration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    user_id = user.id
    text = update.message.text

    step = context.user_data.get("step")

    if step == "name":
        context.user_data["name"] = text
        context.user_data["step"] = "age"
        await update.message.reply_text("Enter your age:")
        return

    if step == "age":
        if not text.isdigit():
            await update.message.reply_text("Enter valid age:")
            return
        context.user_data["age"] = text
        context.user_data["step"] = "gender"
        await update.message.reply_text("Select gender:", reply_markup=gender_keyboard)
        return

    if step == "gender":
        if text not in ["👨 Male", "👩 Female"]:
            await update.message.reply_text("Use buttons.")
            return

        context.user_data["gender"] = "Male" if text == "👨 Male" else "Female"
        context.user_data["step"] = "country"
        await update.message.reply_text("Enter your country:", reply_markup=ReplyKeyboardRemove())
        return

    if step == "country":
        if cursor:
            cursor.execute("""
            INSERT INTO users (user_id, username, name, age, gender, country)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO NOTHING
            """, (
                user_id,
                context.user_data["username"],
                context.user_data["name"],
                context.user_data["age"],
                context.user_data["gender"],
                text
            ))

        context.user_data.clear()

        await update.message.reply_text(
            "Profile saved permanently!",
            reply_markup=main_keyboard
        )

# ================= FIND =================
async def find(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    if user_id in active_chats:
        await update.message.reply_text("You are already chatting.")
        return

    if user_id in waiting_users:
        waiting_users.remove(user_id)

    partner = None
    for w in waiting_users:
        if w != user_id:
            partner = w
            break

    if partner:
        waiting_users.remove(partner)
        active_chats[user_id] = partner
        active_chats[partner] = user_id

        await context.bot.send_message(user_id, "Connected anonymously!")
        await context.bot.send_message(partner, "Connected anonymously!")
    else:
        waiting_users.append(user_id)
        await update.message.reply_text("Waiting for partner...")

# ================= STOP =================
async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    if user_id in active_chats:
        partner = active_chats[user_id]

        del active_chats[user_id]
        del active_chats[partner]

        await context.bot.send_message(user_id, "Chat ended.")
        await context.bot.send_message(partner, "Stranger left the chat.")
    else:
        await update.message.reply_text("You are not in chat.")

# ================= CHAT =================
async def chat_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    user_id = user.id

    if cursor:
        cursor.execute("SELECT * FROM users WHERE user_id=%s", (user_id,))
        existing = cursor.fetchone()
    else:
        existing = None

    if not existing:
        await registration(update, context)
        return

    if user_id not in active_chats:
        await update.message.reply_text(
            "❌ You are not currently connected.\nPress /find to connect.",
            reply_markup=main_keyboard
        )
        return

    partner = active_chats[user_id]

    if cursor:
        cursor.execute("""
        INSERT INTO messages (sender_id, receiver_id, content)
        VALUES (%s, %s, %s)
        """, (user_id, partner, update.message.text or ""))

        cursor.execute("""
        UPDATE users SET total_messages = total_messages + 1
        WHERE user_id=%s
        """, (user_id,))

    await update.message.copy(chat_id=partner)

# ================= ADMIN ANALYTICS =================
async def analytics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return

    if cursor:
        cursor.execute("SELECT COUNT(*) FROM users")
        total_users = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM messages")
        total_messages = cursor.fetchone()[0]
    else:
        total_users = 0
        total_messages = 0

    await update.message.reply_text(
        f"📊 Analytics\n\nUsers: {total_users}\nMessages: {total_messages}"
    )

# ================= RUN =================
app = ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("find", find))
app.add_handler(CommandHandler("stop", stop))
app.add_handler(CommandHandler("analytics", analytics))
app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, chat_handler))

app.run_polling(drop_pending_updates=True)
