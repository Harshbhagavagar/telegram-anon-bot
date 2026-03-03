import os
import psycopg2
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

# ================= CONFIG =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = 643086953  # YOUR TELEGRAM ID

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
    total_messages INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS waiting_users (
    user_id BIGINT PRIMARY KEY
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

# ================= KEYBOARD =================
main_keyboard = ReplyKeyboardMarkup(
    [["/find", "/next"], ["/stop"]],
    resize_keyboard=True
)

# ================= START =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user

    cursor.execute("""
    INSERT INTO users (user_id, username)
    VALUES (%s, %s)
    ON CONFLICT (user_id) DO NOTHING
    """, (user.id, user.username))

    await update.message.reply_text(
        "Welcome to Anonymous Chat!\nUse /find to connect.",
        reply_markup=main_keyboard
    )

# ================= FIND =================
async def find(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    # Already chatting?
    cursor.execute("SELECT partner_id FROM active_chats WHERE user_id=%s", (user_id,))
    if cursor.fetchone():
        await update.message.reply_text("You are already connected.")
        return

    # Remove from waiting if exists
    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (user_id,))

    # Find partner
    cursor.execute("SELECT user_id FROM waiting_users WHERE user_id != %s LIMIT 1", (user_id,))
    row = cursor.fetchone()

    if row:
        partner = row[0]

        cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (partner,))

        cursor.execute("INSERT INTO active_chats VALUES (%s, %s)", (user_id, partner))
        cursor.execute("INSERT INTO active_chats VALUES (%s, %s)", (partner, user_id))

        await context.bot.send_message(user_id, "Connected anonymously!")
        await context.bot.send_message(partner, "Connected anonymously!")
    else:
        cursor.execute("INSERT INTO waiting_users VALUES (%s) ON CONFLICT DO NOTHING", (user_id,))
        await update.message.reply_text("Waiting for partner...")

# ================= STOP =================
async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    cursor.execute("SELECT partner_id FROM active_chats WHERE user_id=%s", (user_id,))
    row = cursor.fetchone()

    if not row:
        await update.message.reply_text("You are not connected.")
        return

    partner = row[0]

    cursor.execute("DELETE FROM active_chats WHERE user_id=%s", (user_id,))
    cursor.execute("DELETE FROM active_chats WHERE user_id=%s", (partner,))

    await context.bot.send_message(user_id, "Chat ended.")
    await context.bot.send_message(partner, "Stranger left the chat.")

# ================= NEXT =================
async def next_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await stop(update, context)
    await find(update, context)

# ================= MESSAGE HANDLER =================
async def chat_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    cursor.execute("SELECT partner_id FROM active_chats WHERE user_id=%s", (user_id,))
    row = cursor.fetchone()

    if not row:
        await update.message.reply_text("Not connected. Use /find.")
        return

    partner = row[0]

    # Determine content type
    if update.message.text:
        content_type = "text"
        content = update.message.text
    elif update.message.sticker:
        content_type = "sticker"
        content = update.message.sticker.file_id
    elif update.message.photo:
        content_type = "photo"
        content = update.message.photo[-1].file_id
    elif update.message.voice:
        content_type = "voice"
        content = update.message.voice.file_id
    elif update.message.video:
        content_type = "video"
        content = update.message.video.file_id
    elif update.message.document:
        content_type = "document"
        content = update.message.document.file_id
    else:
        content_type = "other"
        content = "unsupported"

    # Store message
    cursor.execute("""
    INSERT INTO messages (sender_id, receiver_id, content_type, content)
    VALUES (%s, %s, %s, %s)
    """, (user_id, partner, content_type, content))

    cursor.execute("""
    UPDATE users SET total_messages = total_messages + 1
    WHERE user_id=%s
    """, (user_id,))

    # Forward message
    await update.message.copy(chat_id=partner)

# ================= ADMIN ANALYTICS =================
async def analytics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return

    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM messages")
    total_messages = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM active_chats")
    active_sessions = cursor.fetchone()[0] // 2

    await update.message.reply_text(
        f"📊 Admin Analytics\n\n"
        f"Users: {total_users}\n"
        f"Messages: {total_messages}\n"
        f"Active Chats: {active_sessions}"
    )

# ================= VIEW CHAT LOG =================
async def view_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("Usage: /logs user_id")
        return

    target = int(context.args[0])

    cursor.execute("""
    SELECT sender_id, content_type, content
    FROM messages
    WHERE sender_id=%s OR receiver_id=%s
    ORDER BY sent_at DESC LIMIT 20
    """, (target, target))

    rows = cursor.fetchall()

    if not rows:
        await update.message.reply_text("No messages found.")
        return

    text = "Last 20 messages:\n\n"
    for r in rows:
        text += f"From {r[0]} | {r[1]} | {r[2]}\n"

    await update.message.reply_text(text)

# ================= RUN =================
app = ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("find", find))
app.add_handler(CommandHandler("stop", stop))
app.add_handler(CommandHandler("next", next_chat))
app.add_handler(CommandHandler("analytics", analytics))
app.add_handler(CommandHandler("logs", view_logs))
app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, chat_handler))

app.run_polling(drop_pending_updates=True)
