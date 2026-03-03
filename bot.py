import os
import psycopg2
from telegram import Update, ReplyKeyboardMarkup
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

# ================= TABLES =================
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    username TEXT,
    gender TEXT,
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
    [["/find", "/next"], ["/stop"]],
    resize_keyboard=True
)

vip_keyboard = ReplyKeyboardMarkup(
    [["Match Male", "Match Female"], ["Random Match"]],
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

    cursor.execute("""
    INSERT INTO users (user_id, username)
    VALUES (%s, %s)
    ON CONFLICT (user_id) DO NOTHING
    """, (user.id, user.username))

    cursor.execute("SELECT gender FROM users WHERE user_id=%s", (user.id,))
    if not cursor.fetchone()[0]:
        await update.message.reply_text("Select your gender:", reply_markup=gender_keyboard)
        context.user_data["set_gender"] = True
        return

    await update.message.reply_text("Welcome!", reply_markup=main_keyboard)

# ================= SET GENDER =================
async def set_gender(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("set_gender"):
        gender = update.message.text
        if gender not in ["Male", "Female"]:
            return
        cursor.execute("UPDATE users SET gender=%s WHERE user_id=%s",
                       (gender, update.message.from_user.id))
        context.user_data.clear()
        await update.message.reply_text("Profile saved!", reply_markup=main_keyboard)

# ================= FIND =================
async def find(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    cursor.execute("SELECT is_vip FROM users WHERE user_id=%s", (user_id,))
    is_vip = cursor.fetchone()[0]

    if is_vip:
        await update.message.reply_text("Choose match type:", reply_markup=vip_keyboard)
    else:
        await random_match(update, context)

# ================= RANDOM MATCH =================
async def random_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    await match_user(update, context, None)

# ================= VIP GENDER MATCH =================
async def vip_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    cursor.execute("SELECT is_vip FROM users WHERE user_id=%s", (user_id,))
    is_vip = cursor.fetchone()[0]

    if not is_vip:
        await update.message.reply_text("Please contact admin for VIP.")
        return

    choice = update.message.text

    if choice == "Match Male":
        await match_user(update, context, "Male")
    elif choice == "Match Female":
        await match_user(update, context, "Female")
    elif choice == "Random Match":
        await match_user(update, context, None)

# ================= MATCH FUNCTION =================
async def match_user(update, context, preferred_gender):
    user_id = update.message.from_user.id

    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (user_id,))
    cursor.execute("DELETE FROM active_chats WHERE user_id=%s", (user_id,))

    if preferred_gender:
        cursor.execute("""
        SELECT w.user_id FROM waiting_users w
        JOIN users u ON w.user_id=u.user_id
        WHERE u.gender=%s AND w.user_id != %s LIMIT 1
        """, (preferred_gender, user_id))
    else:
        cursor.execute("""
        SELECT user_id FROM waiting_users
        WHERE user_id != %s LIMIT 1
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
        cursor.execute("INSERT INTO waiting_users VALUES (%s, %s)",
                       (user_id, preferred_gender))
        await update.message.reply_text("Waiting for partner...")

# ================= STOP =================
async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    cursor.execute("SELECT partner_id FROM active_chats WHERE user_id=%s", (user_id,))
    row = cursor.fetchone()
    if not row:
        await update.message.reply_text("Not connected.")
        return

    partner = row[0]

    cursor.execute("DELETE FROM active_chats WHERE user_id=%s", (user_id,))
    cursor.execute("DELETE FROM active_chats WHERE user_id=%s", (partner,))

    await context.bot.send_message(user_id, "Chat ended.")
    await context.bot.send_message(partner, "Stranger left.")

# ================= NEXT =================
async def next_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await stop(update, context)
    await random_match(update, context)

# ================= CHAT =================
async def chat_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    if context.user_data.get("set_gender"):
        await set_gender(update, context)
        return

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
    active_users = cursor.fetchone()[0] // 2

    await update.message.reply_text(
        f"Users: {total_users}\nActive Chats: {active_users}"
    )

async def make_vip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return
    user_id = int(context.args[0])
    cursor.execute("UPDATE users SET is_vip=TRUE WHERE user_id=%s", (user_id,))
    await update.message.reply_text("User is now VIP.")

async def remove_vip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return
    user_id = int(context.args[0])
    cursor.execute("UPDATE users SET is_vip=FALSE WHERE user_id=%s", (user_id,))
    await update.message.reply_text("VIP removed.")

# ================= RUN =================
app = ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("find", find))
app.add_handler(CommandHandler("stop", stop))
app.add_handler(CommandHandler("next", next_chat))
app.add_handler(CommandHandler("analytics", analytics))
app.add_handler(CommandHandler("vip", make_vip))
app.add_handler(CommandHandler("unvip", remove_vip))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, vip_match))
app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, chat_handler))

app.run_polling(drop_pending_updates=True)
