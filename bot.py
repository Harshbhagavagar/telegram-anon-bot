import os
import psycopg2
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

# ================= ENV =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

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
    name TEXT,
    age TEXT,
    gender TEXT,
    country TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# ================= MEMORY =================
waiting_users = []
active_chats = {}

# ================= KEYBOARDS =================
main_keyboard = ReplyKeyboardMarkup(
    [["/find", "/next", "/stop"]],
    resize_keyboard=True
)

gender_keyboard = ReplyKeyboardMarkup(
    [["👨 Male", "👩 Female"]],
    one_time_keyboard=True,
    resize_keyboard=True
)

# ================= ADMIN =================
ADMIN_ID = 123456789  # 🔥 Replace with your Telegram ID


# ================= START =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    cursor.execute("SELECT * FROM users WHERE user_id=%s", (user_id,))
    user = cursor.fetchone()

    if user:
        await update.message.reply_text(
            "Welcome back!",
            reply_markup=main_keyboard
        )
        return

    context.user_data["step"] = "name"
    await update.message.reply_text("Welcome! What is your name?")


# ================= PROFILE SETUP =================
async def collect_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    text = update.message.text

    cursor.execute("SELECT * FROM users WHERE user_id=%s", (user_id,))
    existing_user = cursor.fetchone()

    # Already registered
    if existing_user:
        if user_id in active_chats:
            partner = active_chats[user_id]
            await update.message.copy(chat_id=partner)
        else:
            await update.message.reply_text(
                "❌ You are not currently connected.\nPress /find to connect.",
                reply_markup=main_keyboard
            )
        return

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
            await update.message.reply_text("Select using buttons.")
            return

        gender = "Male" if text == "👨 Male" else "Female"
        context.user_data["gender"] = gender
        context.user_data["step"] = "country"

        await update.message.reply_text(
            "Enter your country:",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if step == "country":
        cursor.execute("""
        INSERT INTO users (user_id, name, age, gender, country)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO NOTHING
        """, (
            user_id,
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
        return


# ================= FIND =================
async def find(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    cursor.execute("SELECT * FROM users WHERE user_id=%s", (user_id,))
    user = cursor.fetchone()

    if not user:
        await update.message.reply_text("Complete profile first with /start")
        return

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


# ================= NEXT =================
async def next_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    if user_id in active_chats:
        partner = active_chats[user_id]
        del active_chats[user_id]
        del active_chats[partner]
        await context.bot.send_message(partner, "Stranger skipped.")

    if user_id in waiting_users:
        waiting_users.remove(user_id)

    await find(update, context)


# ================= ADMIN VIEW USERS =================
async def view_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        await update.message.reply_text("Not authorized.")
        return

    cursor.execute("SELECT user_id, name, age, gender, country FROM users")
    users = cursor.fetchall()

    if not users:
        await update.message.reply_text("No users found.")
        return

    msg = "Registered Users:\n\n"
    for u in users:
        msg += f"{u[0]} | {u[1]} | {u[2]} | {u[3]} | {u[4]}\n"

    await update.message.reply_text(msg[:4000])


# ================= RUN =================
app = ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("find", find))
app.add_handler(CommandHandler("stop", stop))
app.add_handler(CommandHandler("next", next_chat))
app.add_handler(CommandHandler("users", view_users))

app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, collect_data))
app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, collect_data))

app.run_polling()
