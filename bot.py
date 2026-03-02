from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters
import sqlite3
import os

BOT_TOKEN = os.getenv("BOT_TOKEN")


# ---------------- DATABASE ----------------
conn = sqlite3.connect("bot.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    name TEXT,
    age TEXT,
    gender TEXT,
    country TEXT
)
""")
conn.commit()

# ---------------- MEMORY ----------------
waiting_users = []
active_chats = {}

# ---------------- KEYBOARDS ----------------
main_keyboard = ReplyKeyboardMarkup(
    [["/find", "/next", "/stop"]],
    resize_keyboard=True
)

gender_keyboard = ReplyKeyboardMarkup(
    [["👨 Male", "👩 Female"]],
    one_time_keyboard=True,
    resize_keyboard=True
)

# ---------------- START ----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    cursor.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
    user = cursor.fetchone()

    if user:
        await update.message.reply_text("Welcome back!", reply_markup=main_keyboard)
        return

    context.user_data["step"] = "name"
    await update.message.reply_text("Welcome! What is your name?")


# ---------------- PROFILE SETUP ----------------
async def collect_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    text = update.message.text

    cursor.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
    existing_user = cursor.fetchone()

    if existing_user:
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

        await update.message.reply_text("Enter your country:", reply_markup=ReplyKeyboardRemove())
        return

    if step == "country":
        cursor.execute(
            "INSERT INTO users VALUES (?, ?, ?, ?, ?)",
            (
                user_id,
                context.user_data["name"],
                context.user_data["age"],
                context.user_data["gender"],
                text
            )
        )
        conn.commit()

        context.user_data.clear()

        await update.message.reply_text(
            "Profile saved successfully!",
            reply_markup=main_keyboard
        )
        return


# ---------------- FIND ----------------
async def find(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    cursor.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
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
    for waiting_user in waiting_users:
        if waiting_user != user_id:
            partner = waiting_user
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


# ---------------- STOP ----------------
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


# ---------------- NEXT ----------------
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


# ---------------- MESSAGE + MEDIA HANDLER ----------------
async def relay_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    if user_id not in active_chats:
        await update.message.reply_text("❌ You are not currently connected.\nPress /find to connect.")
        return

    partner = active_chats.get(user_id)

    # If partner disconnected unexpectedly
    if not partner or partner not in active_chats:
        await update.message.reply_text("❌ Partner disconnected. Press /find to connect again.")
        return

    # Forward message safely
    try:
        await update.message.copy(chat_id=partner)
    except:
        await update.message.reply_text("⚠️ Failed to send message.")


# ---------------- RUN ----------------
app = ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("find", find))
app.add_handler(CommandHandler("stop", stop))
app.add_handler(CommandHandler("next", next_chat))

# Text, photo, video, document, sticker etc
app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, relay_all))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, collect_data))


app.run_polling()
