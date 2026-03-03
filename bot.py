import os
import psycopg2
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

# ================= HELPERS =================
def get_partner(user_id):
    cursor.execute("SELECT partner_id FROM active_chats WHERE user_id=%s", (user_id,))
    return cursor.fetchone()

def user_exists(user_id):
    cursor.execute("SELECT 1 FROM users WHERE user_id=%s", (user_id,))
    return cursor.fetchone() is not None

# ================= START =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user

    if user_exists(user.id):
        await update.message.reply_text("Welcome back!", reply_markup=main_keyboard)
        return

    context.user_data.clear()
    context.user_data["step"] = "name"
    await update.message.reply_text("Enter your name:")

# ================= MESSAGE ROUTER =================
async def message_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    user = update.message.from_user
    user_id = user.id
    text = update.message.text

    # ===== REGISTRATION FLOW =====
    if context.user_data.get("step"):

        step = context.user_data["step"]

        if step == "name":
            context.user_data["name"] = text
            context.user_data["step"] = "gender"
            await update.message.reply_text("Select gender:", reply_markup=gender_keyboard)
            return

        if step == "gender":
            if text not in ["Male", "Female"]:
                await update.message.reply_text("Please choose Male or Female.")
                return

            context.user_data["gender"] = text
            context.user_data["step"] = "country"
            await update.message.reply_text("Enter country:", reply_markup=ReplyKeyboardRemove())
            return

        if step == "country":
            cursor.execute("""
            INSERT INTO users (user_id, username, name, gender, country)
            VALUES (%s,%s,%s,%s,%s)
            """, (
                user_id,
                user.username,
                context.user_data["name"],
                context.user_data["gender"],
                text
            ))

            context.user_data.clear()
            await update.message.reply_text("✅ Registration complete!", reply_markup=main_keyboard)
            return

    # ===== BLOCK IF NOT REGISTERED =====
    if not user_exists(user_id):
        await update.message.reply_text("Please use /start first.")
        return

    # ===== BUTTON LOGIC =====
    if text in ["Find Partner", "Find Male", "Find Female"]:
        partner = get_partner(user_id)
        if partner:
            await update.message.reply_text(
                "⚠️ You are already in a chat.\nPress Stop first.",
                reply_markup=main_keyboard
            )
            return

        preferred_gender = None
        if text == "Find Male":
            preferred_gender = "Male"
        elif text == "Find Female":
            preferred_gender = "Female"

        await match_user(update, context, preferred_gender)
        return

    if text == "Stop":
        await stop_chat(update, context)
        return

    if text == "Next":
        await stop_chat(update, context)
        await match_user(update, context)
        return

    # ===== CHAT FORWARDING =====
    partner = get_partner(user_id)
    if partner:
        partner_id = partner[0]
        cursor.execute(
            "UPDATE users SET total_messages = total_messages + 1 WHERE user_id=%s",
            (user_id,)
        )
        await update.message.copy(chat_id=partner_id)

# ================= MATCH =================
async def match_user(update, context, preferred_gender=None):
    user_id = update.message.from_user.id

    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (user_id,))

    cursor.execute("SELECT gender FROM users WHERE user_id=%s", (user_id,))
    my_gender = cursor.fetchone()[0]

    if preferred_gender:
        cursor.execute("""
        SELECT w.user_id
        FROM waiting_users w
        JOIN users u ON w.user_id = u.user_id
        WHERE u.gender=%s
        AND w.user_id!=%s
        AND (w.preferred_gender IS NULL OR w.preferred_gender=%s)
        LIMIT 1
        """, (preferred_gender, user_id, my_gender))
    else:
        cursor.execute("""
        SELECT w.user_id
        FROM waiting_users w
        JOIN users u ON w.user_id = u.user_id
        WHERE w.user_id!=%s
        AND (w.preferred_gender IS NULL OR w.preferred_gender=%s)
        LIMIT 1
        """, (user_id, my_gender))

    row = cursor.fetchone()

    if row:
        partner = row[0]

        cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (partner,))
        cursor.execute("INSERT INTO active_chats VALUES (%s,%s)", (user_id, partner))
        cursor.execute("INSERT INTO active_chats VALUES (%s,%s)", (partner, user_id))

        await context.bot.send_message(user_id, "✅ Connected!")
        await context.bot.send_message(partner, "✅ Connected!")
    else:
        cursor.execute("INSERT INTO waiting_users VALUES (%s,%s)", (user_id, preferred_gender))
        await update.message.reply_text("🔎 Searching...")

# ================= STOP =================
async def stop_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    partner = get_partner(user_id)

    if not partner:
        await update.message.reply_text("Not connected.", reply_markup=main_keyboard)
        return

    partner_id = partner[0]

    cursor.execute("DELETE FROM active_chats WHERE user_id=%s", (user_id,))
    cursor.execute("DELETE FROM active_chats WHERE user_id=%s", (partner_id,))

    await context.bot.send_message(user_id, "❌ Chat ended.", reply_markup=main_keyboard)
    await context.bot.send_message(partner_id, "Stranger left.")

# ================= RUN =================
app = ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, message_router))

app.run_polling(drop_pending_updates=True, close_loop=False)
