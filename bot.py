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
def user_exists(user_id: int) -> bool:
    cursor.execute("SELECT 1 FROM users WHERE user_id=%s", (user_id,))
    return cursor.fetchone() is not None

def get_partner_row(user_id: int):
    cursor.execute("SELECT partner_id FROM active_chats WHERE user_id=%s", (user_id,))
    return cursor.fetchone()  # returns (partner_id,) or None

def is_vip(user_id: int) -> bool:
    cursor.execute("SELECT is_vip FROM users WHERE user_id=%s", (user_id,))
    row = cursor.fetchone()
    return bool(row[0]) if row else False

# ================= START =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user = update.message.from_user

    if user_exists(user.id):
        await update.message.reply_text("Welcome back!", reply_markup=main_keyboard)
        return

    # Begin registration flow
    context.user_data.clear()
    context.user_data["step"] = "name"
    await update.message.reply_text("Enter your name:")

# ================= MATCH (mutual matching) =================
async def match_user(update: Update, context: ContextTypes.DEFAULT_TYPE, preferred_gender: str | None = None):
    if not update.message:
        return
    user_id = update.message.from_user.id

    # Prevent searching if already in chat
    if get_partner_row(user_id):
        await update.message.reply_text("⚠️ You are already in a chat. Press Stop first.", reply_markup=main_keyboard)
        return

    # Remove any existing waiting row for this user
    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (user_id,))

    # Get current user's gender (needed for mutual matching)
    cursor.execute("SELECT gender FROM users WHERE user_id=%s", (user_id,))
    my_gender_row = cursor.fetchone()
    if not my_gender_row:
        # user must be registered; this should not happen because router checks, but safe guard
        await update.message.reply_text("Please use /start first.")
        return
    my_gender = my_gender_row[0]

    # Mutual matching SQL:
    # If preferred_gender is provided (user asked specifically), find a waiting user who:
    # - Has gender = preferred_gender
    # - Is not the same user
    # - And either has no preferred_gender or prefers my_gender (mutual)
    if preferred_gender:
        cursor.execute("""
            SELECT w.user_id
            FROM waiting_users w
            JOIN users u ON w.user_id = u.user_id
            WHERE u.gender = %s
              AND w.user_id != %s
              AND (w.preferred_gender IS NULL OR w.preferred_gender = %s)
            LIMIT 1
        """, (preferred_gender, user_id, my_gender))
    else:
        # No specific preference: accept any waiting user who either has no preference or prefers my_gender
        cursor.execute("""
            SELECT w.user_id
            FROM waiting_users w
            JOIN users u ON w.user_id = u.user_id
            WHERE w.user_id != %s
              AND (w.preferred_gender IS NULL OR w.preferred_gender = %s)
            LIMIT 1
        """, (user_id, my_gender))

    row = cursor.fetchone()

    if row:
        partner = row[0]
        # Create active chat pair
        cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (partner,))
        # Insert both directions. If there is a race, one insert might fail in rare cases; keep it simple:
        cursor.execute("INSERT INTO active_chats (user_id, partner_id) VALUES (%s,%s) ON CONFLICT (user_id) DO UPDATE SET partner_id = EXCLUDED.partner_id", (user_id, partner))
        cursor.execute("INSERT INTO active_chats (user_id, partner_id) VALUES (%s,%s) ON CONFLICT (user_id) DO UPDATE SET partner_id = EXCLUDED.partner_id", (partner, user_id))

        await context.bot.send_message(user_id, "✅ Connected!")
        await context.bot.send_message(partner, "✅ Connected!")
    else:
        # No partner found: join waiting list
        cursor.execute("INSERT INTO waiting_users (user_id, preferred_gender) VALUES (%s,%s) ON CONFLICT (user_id) DO UPDATE SET preferred_gender = EXCLUDED.preferred_gender", (user_id, preferred_gender))
        await update.message.reply_text("🔎 Searching...")

# ================= STOP =================
async def stop_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user_id = update.message.from_user.id

    row = get_partner_row(user_id)
    if not row:
        await update.message.reply_text("Not connected.", reply_markup=main_keyboard)
        return

    partner_id = row[0]

    cursor.execute("DELETE FROM active_chats WHERE user_id=%s", (user_id,))
    cursor.execute("DELETE FROM active_chats WHERE user_id=%s", (partner_id,))

    await context.bot.send_message(user_id, "❌ Chat ended.", reply_markup=main_keyboard)
    # partner might not be present (rare), wrap in try/except to avoid crash
    try:
        await context.bot.send_message(partner_id, "Stranger left.")
    except Exception:
        pass

# ================= MESSAGE ROUTER =================
async def message_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    user = update.message.from_user
    user_id = user.id
    text = update.message.text

    # --- REGISTRATION FLOW FIRST ---
    if context.user_data.get("step"):
        step = context.user_data["step"]

        if step == "name":
            # store name
            context.user_data["name"] = text
            context.user_data["step"] = "gender"
            await update.message.reply_text("Select gender:", reply_markup=gender_keyboard)
            return

        if step == "gender":
            if text not in ("Male", "Female"):
                await update.message.reply_text("Please choose Male or Female.", reply_markup=gender_keyboard)
                return
            context.user_data["gender"] = text
            context.user_data["step"] = "country"
            await update.message.reply_text("Enter country:", reply_markup=ReplyKeyboardRemove())
            return

        if step == "country":
            # insert user in DB
            cursor.execute("""
                INSERT INTO users (user_id, username, name, gender, country)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (user_id) DO NOTHING
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

    # --- BLOCK if not registered ---
    if not user_exists(user_id):
        await update.message.reply_text("Please use /start first.")
        return

    # --- BUTTONS: VIP check for gender-specific searches ---
    # Determine if this is a gender-specific search
    if text == "Find Partner":
        # general search (allowed for all)
        if get_partner_row(user_id):
            await update.message.reply_text("⚠️ You are already in a chat. Press Stop first.", reply_markup=main_keyboard)
            return
        await match_user(update, context, None)
        return

    if text == "Find Male" or text == "Find Female":
        # VIP required for gender-specific
        if not is_vip(user_id):
            await update.message.reply_text("👑 VIP required. Please contact admin.@Random1204", reply_markup=main_keyboard)
            return

        if get_partner_row(user_id):
            await update.message.reply_text("⚠️ You are already in a chat. Press Stop first.", reply_markup=main_keyboard)
            return

        preferred = "Male" if text == "Find Male" else "Female"
        await match_user(update, context, preferred)
        return

    if text == "Next":
        # Next should stop current chat then search again (if not in chat, it will just search)
        if get_partner_row(user_id):
            # stop current chat first
            await stop_chat(update, context)
        # Now find new partner (no preference)
        await match_user(update, context, None)
        return

    if text == "Stop":
        await stop_chat(update, context)
        return

    # --- NORMAL CHAT: forward everything to partner if in chat ---
    partner_row = get_partner_row(user_id)
    if partner_row:
        partner_id = partner_row[0]
        # update counters safely
        try:
            cursor.execute("UPDATE users SET total_messages = total_messages + 1 WHERE user_id=%s", (user_id,))
        except Exception:
            pass
        # forward the message (works for all media types)
        try:
            await update.message.copy(chat_id=partner_id)
        except Exception:
            # if forwarding fails, notify user and cleanup maybe
            await update.message.reply_text("Failed to forward message. Try again or press Stop.")
        return

    # If not in chat and message is non-button free text, optionally inform or ignore
    # We'll inform user how to start
    await update.message.reply_text("Use the keyboard to find a partner or press /start to (re)register.", reply_markup=main_keyboard)

# ================= ADMIN ANALYTICS (optional) =================
async def analytics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if update.message.from_user.id != ADMIN_ID:
        return

    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM active_chats")
    active_pairs = cursor.fetchone()[0] // 2

    await update.message.reply_text(f"📊 Users: {total_users}\nActive chats: {active_pairs}")

# ================= RUN =================
app = ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("analytics", analytics))
app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, message_router))

app.run_polling(drop_pending_updates=True, close_loop=False)
