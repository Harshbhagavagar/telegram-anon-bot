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

# ===== SAFE AUTO MIGRATION FOR REFERRAL =====
cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by BIGINT")
cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_count INT DEFAULT 0")
cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS vip_expiry TIMESTAMP")

# ================= KEYBOARDS =================
main_keyboard = ReplyKeyboardMarkup(
    [
        ["Find Partner"],
        ["Find Male", "Find Female"],
        ["Next", "Stop"],
        ["My Referral"]
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
    return cursor.fetchone()

def is_vip(user_id: int) -> bool:
    cursor.execute("SELECT is_vip, vip_expiry FROM users WHERE user_id=%s", (user_id,))
    row = cursor.fetchone()
    if not row:
        return False
    is_vip_flag, vip_expiry = row
    if vip_expiry and vip_expiry > datetime.utcnow():
        return True
    return is_vip_flag

def reward_referrer(referrer_id: int):
    cursor.execute("SELECT referral_count FROM users WHERE user_id=%s", (referrer_id,))
    row = cursor.fetchone()
    if not row:
        return
    count = row[0]

    days = 0
    if count == 3:
        days = 3
    elif count == 5:
        days = 7
    elif count == 10:
        days = 30

    if days > 0:
        expiry = datetime.utcnow() + timedelta(days=days)
        cursor.execute("UPDATE users SET vip_expiry=%s WHERE user_id=%s", (expiry, referrer_id))

# ================= START =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    user = update.message.from_user
    user_id = user.id

    if user_exists(user_id):
        await update.message.reply_text("Welcome back!", reply_markup=main_keyboard)
        return

    referrer_id = None
    if context.args:
        try:
            possible_ref = int(context.args[0])
            if possible_ref != user_id:
                cursor.execute("SELECT 1 FROM users WHERE user_id=%s", (possible_ref,))
                if cursor.fetchone():
                    referrer_id = possible_ref
        except:
            pass

    context.user_data.clear()
    context.user_data["step"] = "name"
    context.user_data["referrer"] = referrer_id

    await update.message.reply_text("Enter your name:")

# ================= MATCHING =================
async def match_user(update: Update, context: ContextTypes.DEFAULT_TYPE, preferred_gender=None):
    if not update.message:
        return

    user_id = update.message.from_user.id

    if get_partner_row(user_id):
        await update.message.reply_text("⚠️ You are already in a chat. Press Stop first.", reply_markup=main_keyboard)
        return

    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (user_id,))

    cursor.execute("SELECT gender FROM users WHERE user_id=%s", (user_id,))
    my_gender = cursor.fetchone()[0]

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
        cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (partner,))
        cursor.execute("INSERT INTO active_chats (user_id, partner_id) VALUES (%s,%s) ON CONFLICT (user_id) DO UPDATE SET partner_id=EXCLUDED.partner_id", (user_id, partner))
        cursor.execute("INSERT INTO active_chats (user_id, partner_id) VALUES (%s,%s) ON CONFLICT (user_id) DO UPDATE SET partner_id=EXCLUDED.partner_id", (partner, user_id))

        await context.bot.send_message(user_id, "✅ Connected!")
        await context.bot.send_message(partner, "✅ Connected!")
    else:
        cursor.execute("INSERT INTO waiting_users (user_id, preferred_gender) VALUES (%s,%s) ON CONFLICT (user_id) DO UPDATE SET preferred_gender=EXCLUDED.preferred_gender", (user_id, preferred_gender))
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
    try:
        await context.bot.send_message(partner_id, "Stranger left.")
    except:
        pass

# ================= MESSAGE ROUTER =================
async def message_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    user = update.message.from_user
    user_id = user.id
    text = update.message.text

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
            referrer_id = context.user_data.get("referrer")

            cursor.execute("""
                INSERT INTO users (user_id, username, name, gender, country, referred_by)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (user_id) DO NOTHING
            """, (
                user_id,
                user.username,
                context.user_data["name"],
                context.user_data["gender"],
                text,
                referrer_id
            ))

            if referrer_id:
                cursor.execute("UPDATE users SET referral_count = referral_count + 1 WHERE user_id=%s", (referrer_id,))
                reward_referrer(referrer_id)

            context.user_data.clear()
            await update.message.reply_text("✅ Registration complete!", reply_markup=main_keyboard)
            return

    if not user_exists(user_id):
        await update.message.reply_text("Please use /start first.")
        return

    # ===== REFERRAL BUTTON =====
    if text == "My Referral":
        referral_link = f"https://t.me/{update.message.bot.username}?start={user_id}"
        cursor.execute("SELECT referral_count FROM users WHERE user_id=%s", (user_id,))
        count = cursor.fetchone()[0]

        await update.message.reply_text(
            f"🔗 Your Referral Link:\n{referral_link}\n\n"
            f"👥 Referrals: {count}\n\n"
            "🎁 3 invites = 3 days VIP\n"
            "5 invites = 7 days VIP\n"
            "10 invites = 30 days VIP"
        )
        return

    # ===== MATCHING =====
    if text == "Find Partner":
        await match_user(update, context)
        return

    if text in ("Find Male", "Find Female"):
        if not is_vip(user_id):
            await update.message.reply_text("👑 VIP required. Contact admin.", reply_markup=main_keyboard)
            return
        preferred = "Male" if text == "Find Male" else "Female"
        await match_user(update, context, preferred)
        return

    if text == "Next":
        if get_partner_row(user_id):
            await stop_chat(update, context)
        await match_user(update, context)
        return

    if text == "Stop":
        await stop_chat(update, context)
        return

    # ===== CHAT FORWARD =====
    partner_row = get_partner_row(user_id)
    if partner_row:
        partner_id = partner_row[0]
        cursor.execute("UPDATE users SET total_messages = total_messages + 1 WHERE user_id=%s", (user_id,))
        await update.message.copy(chat_id=partner_id)
        return

    await update.message.reply_text("Use keyboard below.", reply_markup=main_keyboard)

# ================= RUN =================
app = ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, message_router))

app.run_polling(drop_pending_updates=True, close_loop=False)
