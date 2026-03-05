import os
import psycopg2
import asyncio
from datetime import datetime, timedelta
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

# ================= CONFIG =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = 643086953

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cursor = conn.cursor()

# ================= DATABASE SETUP =================
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    username TEXT,
    name TEXT,
    gender TEXT,
    country TEXT,
    age INT,
    is_vip BOOLEAN DEFAULT FALSE,
    vip_expiry TIMESTAMP,
    referral_count INT DEFAULT 0,
    referred_by BIGINT,
    total_messages INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
cursor.execute("CREATE TABLE IF NOT EXISTS waiting_users (user_id BIGINT PRIMARY KEY, preferred_gender TEXT)")
cursor.execute("CREATE TABLE IF NOT EXISTS active_chats (user_id BIGINT PRIMARY KEY, partner_id BIGINT)")

# ================= KEYBOARDS =================
user_keyboard = ReplyKeyboardMarkup([["🚀 Find Partner"], ["👨 Find Male","👩 Find Female"], ["⏭ Next","❌ Stop"], ["💎 VIP"]], resize_keyboard=True)
vip_keyboard = ReplyKeyboardMarkup([["🎁 Get FREE VIP"], ["👑 Contact Admin"], ["⬅ Back"]], resize_keyboard=True)
admin_keyboard = ReplyKeyboardMarkup([["📊 Analytics"], ["📢 Announcement"], ["👥 Active Users","🕒 Waiting Users"], ["⬅ Back"]], resize_keyboard=True)
gender_keyboard = ReplyKeyboardMarkup([["Male","Female"]], resize_keyboard=True, one_time_keyboard=True)

# ================= HELPERS =================
def user_exists(uid):
    cursor.execute("SELECT 1 FROM users WHERE user_id=%s",(uid,))
    return cursor.fetchone() is not None

def get_partner(uid):
    cursor.execute("SELECT partner_id FROM active_chats WHERE user_id=%s",(uid,))
    row=cursor.fetchone()
    return row[0] if row else None

def is_vip(uid):
    cursor.execute("SELECT is_vip, vip_expiry FROM users WHERE user_id=%s",(uid,))
    row=cursor.fetchone()
    if not row: return False
    vip, expiry = row
    if vip: return True
    # Using utcnow() for PostgreSQL naive timestamp compatibility
    if expiry: return expiry > datetime.utcnow()
    return False

# ================= MATCHMAKING =================
async def match_user(update, context, pref=None):
    uid = update.message.from_user.id
    if get_partner(uid): return
    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s",(uid,))
    
    if pref:
        cursor.execute("""
            SELECT w.user_id FROM waiting_users w
            JOIN users u ON w.user_id = u.user_id
            WHERE u.gender = %s AND (w.preferred_gender = %s OR w.preferred_gender IS NULL)
            AND w.user_id != %s LIMIT 1
        """, (pref, pref, uid))
    else:
        cursor.execute("SELECT user_id FROM waiting_users WHERE user_id != %s LIMIT 1", (uid,))
    
    row = cursor.fetchone()
    if row:
        partner = row[0]
        cursor.execute("DELETE FROM waiting_users WHERE user_id=%s",(partner,))
        cursor.execute("INSERT INTO active_chats VALUES(%s,%s), (%s,%s)", (uid, partner, partner, uid))
        await context.bot.send_message(uid, "✅ Connected!")
        await context.bot.send_message(partner, "✅ Connected!")
    else:
        cursor.execute("INSERT INTO waiting_users VALUES(%s,%s)", (uid, pref))
        await update.message.reply_text("🔎 Searching for a partner...")

async def stop_chat(update, context):
    uid = update.message.from_user.id
    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s",(uid,))
    partner = get_partner(uid)
    if not partner:
        await update.message.reply_text("⛔ Search stopped", reply_markup=user_keyboard)
        return
    cursor.execute("DELETE FROM active_chats WHERE user_id IN (%s, %s)", (uid, partner))
    await update.message.reply_text("❌ Chat ended", reply_markup=user_keyboard)
    try: await context.bot.send_message(partner, "Stranger left the chat")
    except: pass

# ================= ROUTER =================

async def router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message: return
    uid, text = update.message.from_user.id, update.message.text or ""

    # 1. FIXED BACK BUTTON (Handles Admin correctly)
    if text == "⬅ Back":
        context.user_data.pop("announce_mode", None)
        if uid == ADMIN_ID:
            await update.message.reply_text("👑 Admin Main Menu", reply_markup=admin_keyboard)
        else:
            await update.message.reply_text("🏠 Main Menu", reply_markup=user_keyboard)
        return

    # 2. REGISTRATION FLOW
    step = context.user_data.get("step")
    if step == "name":
        context.user_data.update({"name": text, "step": "gender"})
        await update.message.reply_text("Select your gender", reply_markup=gender_keyboard)
        return
    if step == "gender":
        if text not in ["Male", "Female"]:
            await update.message.reply_text("Use buttons!")
            return
        context.user_data.update({"gender": text, "step": "country"})
        await update.message.reply_text("Enter your country", reply_markup=ReplyKeyboardRemove())
        return
    if step == "country":
        context.user_data.update({"country": text, "step": "age"})
        await update.message.reply_text("Enter your age")
        return
    if step == "age":
        if not text.isdigit():
            await update.message.reply_text("Age must be a number")
            return
        cursor.execute("UPDATE users SET name=%s, gender=%s, country=%s, age=%s WHERE user_id=%s",
                       (context.user_data["name"], context.user_data["gender"], context.user_data["country"], int(text), uid))
        context.user_data.clear()
        await update.message.reply_text("Registration complete 🎉", reply_markup=user_keyboard)
        return

    # 3. ADMIN PANEL
    if uid == ADMIN_ID:
        if text == "📊 Analytics":
            cursor.execute("SELECT COUNT(*) FROM users")
            total = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM active_chats")
            active = cursor.fetchone()[0] // 2
            await update.message.reply_text(f"Users: {total}\nActive chats: {active}")
            return
        
        if text == "👥 Active Users":
            cursor.execute("SELECT COUNT(*) FROM active_chats")
            active = cursor.fetchone()[0] // 2
            await update.message.reply_text(f"Current Active Chats: {active}")
            return

        if text == "🕒 Waiting Users":
            cursor.execute("SELECT COUNT(*) FROM waiting_users")
            waiting = cursor.fetchone()[0]
            await update.message.reply_text(f"Users currently in queue: {waiting}")
            return

        if text == "📢 Announcement":
            context.user_data["announce_mode"] = True
            await update.message.reply_text("📢 Send the message (Text/Photo/Video) to broadcast, or press ⬅ Back to cancel.")
            return

        if context.user_data.get("announce_mode"):
            # Announcement automatically cancels if Back is pressed (handled above)
            context.user_data["announce_mode"] = False
            cursor.execute("SELECT user_id FROM users")
            rows = cursor.fetchall()
            sent = 0
            for r in rows:
                try: 
                    await update.message.copy(chat_id=r[0])
                    sent += 1
                    await asyncio.sleep(0.05)
                except: pass
            await update.message.reply_text(f"✅ Announcement sent to {sent} users!")
            return

    # 4. USER BUTTONS
    if text == "🚀 Find Partner": await match_user(update, context)
    elif text == "👨 Find Male":
        if is_vip(uid): await match_user(update, context, "Male")
        else: await update.message.reply_text("👑 VIP required")
    elif text == "👩 Find Female":
        if is_vip(uid): await match_user(update, context, "Female")
        else: await update.message.reply_text("👑 VIP required")
    elif text == "⏭ Next":
        await stop_chat(update, context); await match_user(update, context)
    elif text == "❌ Stop": await stop_chat(update, context)
    elif text == "💎 VIP": await update.message.reply_text("💎 VIP Menu", reply_markup=vip_keyboard)
    
    # 5. FIXED VIP/REFERRAL PROGRESS
    elif text == "🎁 Get FREE VIP":
        cursor.execute("SELECT referral_count FROM users WHERE user_id=%s", (uid,))
        row = cursor.fetchone()
        count = row[0] if row else 0
        link = f"https://t.me/{context.bot.username}?start={uid}"
        await update.message.reply_text(
            f"🎁 **Invite Friends to get FREE VIP**\n\n"
            f"Your link:\n{link}\n\n"
            f"Progress: {count}/3\n"
            f"Invite 3 friends to unlock 👑 VIP for 3 days!"
        )
    elif text == "👑 Contact Admin":
        await update.message.reply_text("Contact Admin for VIP purchase: @Random1204")

    # 6. FORWARDING
    partner = get_partner(uid)
    blocked = ["🚀 Find Partner", "👨 Find Male", "👩 Find Female", "⏭ Next", "❌ Stop", "💎 VIP", "🎁 Get FREE VIP", "📊 Analytics", "📢 Announcement", "⬅ Back", "👥 Active Users", "🕒 Waiting Users", "👑 Contact Admin"]
    if partner and text not in blocked:
        try: await update.message.copy(chat_id=partner)
        except:
            cursor.execute("DELETE FROM active_chats WHERE user_id IN (%s, %s)", (uid, partner))
            await update.message.reply_text("Partner disconnected")

# ================= START =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.message.from_user.id
    if user_exists(uid):
        cursor.execute("SELECT name, gender, country, age FROM users WHERE user_id=%s", (uid,))
        row = cursor.fetchone()
        if None in row:
            context.user_data["step"] = "name"
            await update.message.reply_text("Please finish your profile. Enter name:")
        else:
            kb = admin_keyboard if uid == ADMIN_ID else user_keyboard
            await update.message.reply_text("Welcome back!", reply_markup=kb)
        return
    
    ref = int(context.args[0]) if context.args and context.args[0].isdigit() else None
    cursor.execute("INSERT INTO users (user_id, username, referred_by) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING", (uid, update.message.from_user.username, ref))
    if ref and ref != uid:
        cursor.execute("UPDATE users SET referral_count = referral_count + 1 WHERE user_id = %s", (ref,))
        cursor.execute("SELECT referral_count FROM users WHERE user_id = %s", (ref,))
        r_row = cursor.fetchone()
        if r_row and r_row[0] >= 3:
            # Grants 3 days VIP
            cursor.execute("UPDATE users SET vip_expiry = %s WHERE user_id = %s", (datetime.utcnow() + timedelta(days=3), ref))
    
    context.user_data["step"] = "name"
    await update.message.reply_text("Welcome! Enter your name to start:")

app = ApplicationBuilder().token(BOT_TOKEN).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, router))
app.run_polling(drop_pending_updates=True)
