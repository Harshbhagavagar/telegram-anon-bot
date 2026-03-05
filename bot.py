import os
import psycopg2
import asyncio
from datetime import datetime, timedelta
from telegram import (
    Update, 
    ReplyKeyboardMarkup, 
    ReplyKeyboardRemove, 
    InlineKeyboardButton, 
    InlineKeyboardMarkup
)
from telegram.ext import (
    ApplicationBuilder, 
    CommandHandler, 
    MessageHandler, 
    CallbackQueryHandler, 
    ContextTypes, 
    filters
)

# ================= CONFIG =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = 643086953

# Stable Connection with Keep-Alives
conn = psycopg2.connect(
    DATABASE_URL,
    keepalives=1,
    keepalives_idle=30,
    keepalives_interval=10,
    keepalives_count=5
)
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
    is_vip BOOLEAN DEFAULT FALSE,
    vip_expiry TIMESTAMP,
    referral_count INT DEFAULT 0,
    referred_by BIGINT,
    total_messages INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
cursor.execute("CREATE TABLE IF NOT EXISTS waiting_users (user_id BIGINT PRIMARY KEY, preferred_gender TEXT, entry_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
cursor.execute("CREATE TABLE IF NOT EXISTS active_chats (user_id BIGINT PRIMARY KEY, partner_id BIGINT)")

# ================= KEYBOARDS =================
user_keyboard = ReplyKeyboardMarkup([
    ["🚀 Find Partner"],
    ["👨 Find Male", "👩 Find Female"],
    ["⏭ Next", "❌ Stop"],
    ["💎 VIP"]
], resize_keyboard=True)

vip_keyboard = ReplyKeyboardMarkup([
    ["🎁 Get FREE VIP"],
    ["👑 Contact Admin"],
    ["⬅ Back"]
], resize_keyboard=True)

admin_keyboard = ReplyKeyboardMarkup([
    ["📊 Analytics"],
    ["👥 Active Users", "🕒 Waiting Users"],
    ["⬅ Back"]
], resize_keyboard=True)

gender_keyboard = ReplyKeyboardMarkup([["Male", "Female"]], resize_keyboard=True, one_time_keyboard=True)

# ================= HELPERS =================
def user_exists(uid):
    cursor.execute("SELECT 1 FROM users WHERE user_id=%s", (uid,))
    return cursor.fetchone() is not None

def get_partner(uid):
    cursor.execute("SELECT partner_id FROM active_chats WHERE user_id=%s", (uid,))
    row = cursor.fetchone()
    return row[0] if row else None

def is_vip(uid):
    cursor.execute("SELECT is_vip, vip_expiry FROM users WHERE user_id=%s", (uid,))
    row = cursor.fetchone()
    if not row: return False
    vip, expiry = row
    return True if vip or (expiry and expiry > datetime.utcnow()) else False

def reward_referral(uid):
    cursor.execute("SELECT referral_count FROM users WHERE user_id=%s", (uid,))
    row = cursor.fetchone()
    if row and row[0] >= 3:
        expiry = datetime.utcnow() + timedelta(days=3)
        cursor.execute("UPDATE users SET vip_expiry=%s WHERE user_id=%s", (expiry, uid))

# ================= ADMIN COMMANDS =================
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID: return
    if not context.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    
    msg = " ".join(context.args)
    cursor.execute("SELECT user_id FROM users")
    rows = cursor.fetchall()
    
    sent = 0
    for r in rows:
        try:
            await context.bot.send_message(chat_id=r[0], text=msg)
            sent += 1
            await asyncio.sleep(0.05) 
        except Exception:
            continue
    await update.message.reply_text(f"✅ Sent to {sent} users")

async def find_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID: return
    if not context.args:
        await update.message.reply_text("Usage: /find <name or id>")
        return
    
    q = context.args[0]
    if q.isdigit():
        cursor.execute("SELECT user_id, name, is_vip FROM users WHERE user_id=%s", (int(q),))
    else:
        cursor.execute("SELECT user_id, name, is_vip FROM users WHERE name ILIKE %s", (f"%{q}%",))
    
    rows = cursor.fetchall()
    if not rows:
        await update.message.reply_text("❌ User not found")
        return

    for uid, name, vip in rows:
        status = "👑 VIP" if vip else "👤 Normal"
        btn_txt = "❌ Remove VIP" if vip else "💎 Make VIP"
        callback = f"unvip_{uid}" if vip else f"vip_{uid}"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(btn_txt, callback_data=callback)]])
        await update.message.reply_text(f"👤 Name: {name}\n🆔 ID: {uid}\n✨ Status: {status}", reply_markup=kb)

async def vip_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    uid = int(data.split("_")[1])

    if data.startswith("vip_"):
        cursor.execute("UPDATE users SET is_vip=TRUE WHERE user_id=%s", (uid,))
        await query.edit_message_text(f"✅ User {uid} is now VIP")
    elif data.startswith("unvip_"):
        cursor.execute("UPDATE users SET is_vip=FALSE WHERE user_id=%s", (uid,))
        await query.edit_message_text(f"❌ VIP removed for {uid}")

# ================= MATCHMAKING =================
async def match_user(update, context, pref=None):
    uid = update.message.from_user.id
    if get_partner(uid): return
    
    # Remove from waiting if they try to search again
    cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (uid,))
    
    sql = "SELECT w.user_id FROM waiting_users w JOIN users u ON w.user_id=u.user_id WHERE u.gender=%s AND w.user_id!=%s ORDER BY w.entry_time ASC LIMIT 1" if pref else "SELECT user_id FROM waiting_users WHERE user_id!=%s ORDER BY entry_time ASC LIMIT 1"
    cursor.execute(sql, (pref, uid) if pref else (uid,))
    row = cursor.fetchone()

    if row:
        partner = row[0]
        cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (partner,))
        cursor.execute("INSERT INTO active_chats VALUES(%s,%s), (%s,%s)", (uid, partner, partner, uid))
        await context.bot.send_message(uid, "✅ Connected!")
        await context.bot.send_message(partner, "✅ Connected!")
    else:
        cursor.execute("""
            INSERT INTO waiting_users (user_id, preferred_gender) 
            VALUES (%s,%s) 
            ON CONFLICT (user_id) DO UPDATE SET preferred_gender=EXCLUDED.preferred_gender
        """, (uid, pref))
        await update.message.reply_text("🔎 Searching for a partner...")
        context.job_queue.run_once(match_timeout, 180, chat_id=uid, user_id=uid)

async def match_timeout(context: ContextTypes.DEFAULT_TYPE):
    uid = context.job.user_id
    # Safer check: only delete if they are still waiting
    cursor.execute("SELECT 1 FROM waiting_users WHERE user_id=%s", (uid,))
    if cursor.fetchone():
        cursor.execute("DELETE FROM waiting_users WHERE user_id=%s", (uid,))
        try:
            await context.bot.send_message(uid, "⏱ No partner found. Try again later!", reply_markup=user_keyboard)
        except Exception: pass

async def stop_chat(update, context):
    uid = update.message.from_user.id
    partner = get_partner(uid)
    if not partner: return
    cursor.execute("DELETE FROM active_chats WHERE user_id IN (%s, %s)", (uid, partner))
    await context.bot.send_message(uid, "❌ Chat ended.", reply_markup=user_keyboard)
    try: await context.bot.send_message(partner, "Stranger left the chat.", reply_markup=user_keyboard)
    except Exception: pass

# ================= ROUTER =================
async def router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text: return
    uid, text = update.message.from_user.id, update.message.text

    if text in ["⬅ Back", "Back"]:
        context.user_data.clear()
        kb = admin_keyboard if uid == ADMIN_ID else user_keyboard
        await update.message.reply_text("Main Menu", reply_markup=kb)
        return

    if uid == ADMIN_ID:
        if text == "📊 Analytics":
            cursor.execute("SELECT COUNT(*) FROM users")
            total = cursor.fetchone()[0]
            cursor.execute("SELECT gender, COUNT(*) FROM users GROUP BY gender")
            genders = dict(cursor.fetchall())
            cursor.execute("SELECT COUNT(*) FROM active_chats")
            active = cursor.fetchone()[0] // 2
            msg = f"📊 BOT ANALYTICS\nUsers: {total}\nChats: {active}\n\n👨 Male: {genders.get('Male',0)}\n👩 Female: {genders.get('Female',0)}"
            await update.message.reply_text(msg)
            return
        if text == "👥 Active Users":
            cursor.execute("SELECT COUNT(*) FROM active_chats")
            await update.message.reply_text(f"Active Chats: {cursor.fetchone()[0]//2}")
            return
        if text == "🕒 Waiting Users":
            cursor.execute("SELECT COUNT(*) FROM waiting_users")
            await update.message.reply_text(f"Waiting Users: {cursor.fetchone()[0]}")
            return

    if context.user_data.get("step"):
        step = context.user_data["step"]
        if step == "name":
            context.user_data.update({"name": text, "step": "gender"})
            await update.message.reply_text("Select gender:", reply_markup=gender_keyboard)
        elif step == "gender":
            gen = text.capitalize()
            if gen not in ["Male", "Female"]:
                await update.message.reply_text("⚠️ Please use buttons.", reply_markup=gender_keyboard)
                return
            context.user_data.update({"gender": gen, "step": "country"})
            await update.message.reply_text("Enter country:", reply_markup=ReplyKeyboardRemove())
        elif step == "country":
            ref = context.user_data.get("ref")
            cursor.execute("INSERT INTO users(user_id,username,name,gender,country,referred_by) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (uid, update.message.from_user.username, context.user_data["name"], context.user_data["gender"], text, ref))
            if ref:
                cursor.execute("UPDATE users SET referral_count=referral_count+1 WHERE user_id=%s", (ref,))
                reward_referral(ref)
            context.user_data.clear()
            await update.message.reply_text("✅ Registration complete!", reply_markup=user_keyboard)
        return

    if text == "🚀 Find Partner": await match_user(update, context)
    elif text == "👨 Find Male": 
        if is_vip(uid): await match_user(update, context, "Male")
        else: await update.message.reply_text("👑 VIP Required.")
    elif text == "👩 Find Female":
        if is_vip(uid): await match_user(update, context, "Female")
        else: await update.message.reply_text("👑 VIP Required.")
    elif text == "⏭ Next":
        await stop_chat(update, context)
        await match_user(update, context)
    elif text == "❌ Stop": await stop_chat(update, context)
    elif text == "💎 VIP": await update.message.reply_text("VIP Features", reply_markup=vip_keyboard)
    elif text == "🎁 Get FREE VIP":
        cursor.execute("SELECT referral_count FROM users WHERE user_id=%s", (uid,))
        row = cursor.fetchone()
        c = row[0] if row else 0
        await update.message.reply_text(f"Link: https://t.me/{context.bot.username}?start={uid}\nProgress: {c}/3")
    
    partner = get_partner(uid)
    if partner:
        try: await update.message.copy(chat_id=partner)
        except Exception:
            cursor.execute("DELETE FROM active_chats WHERE user_id IN (%s, %s)", (uid, partner))
            await update.message.reply_text("Partner disconnected.")
            await match_user(update, context)

# ================= RUN =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.message.from_user.id
    ref = int(context.args[0]) if context.args and context.args[0].isdigit() else None
    if uid == ADMIN_ID:
        await update.message.reply_text("Admin", reply_markup=admin_keyboard)
        return
    if user_exists(uid):
        await update.message.reply_text("Welcome!", reply_markup=user_keyboard)
        return
    context.user_data["step"], context.user_data["ref"] = "name", ref
    await update.message.reply_text("Enter name:")

app = ApplicationBuilder().token(BOT_TOKEN).build()

# Initialize JobQueue correctly
app.job_queue.start()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("broadcast", broadcast))
app.add_handler(CommandHandler("find", find_user))
app.add_handler(CallbackQueryHandler(vip_toggle))
app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, router))



app.run_polling(drop_pending_updates=True)
