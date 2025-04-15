from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    CallbackContext,
    ConversationHandler,
    CallbackQueryHandler,
    PreCheckoutQueryHandler,
)
import logging
import os
import time
from datetime import datetime
import psycopg2
from psycopg2 import pool
from urllib.parse import urlparse
import json
import re
from collections import defaultdict
import warnings
import telegram.error

# Suppress ConversationHandler warning
warnings.filterwarnings("ignore", category=UserWarning, module="telegram.ext.conversationhandler")

# Set up logging for Heroku
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Admin configuration
ADMIN_IDS = {5975525252}  # Replace with your Telegram user ID

# Security configurations
BANNED_WORDS = {
    "spam", "hate", "abuse", "nsfw", "inappropriate", "offensive",
    "harass", "bully", "threat", "sex", "porn", "nude", "violence",
    "scam", "hack", "phish", "malware"
}
COMMAND_COOLDOWN = 5
MAX_MESSAGES_PER_MINUTE = 15
REPORT_THRESHOLD = 3
TEMP_BAN_DURATION = 24 * 3600  # 1 day
MAX_PROFILE_LENGTH = 500
ALLOWED_TAGS = {"music", "gaming", "movies", "tech", "sports", "art", "travel", "food", "books", "fashion"}

# In-memory storage for runtime (non-persistent)
waiting_users = []
user_pairs = {}
previous_partners = {}
command_timestamps = {}
message_timestamps = defaultdict(list)
chat_histories = {}  # Premium feature: in-memory

# Conversation states
GENDER, AGE, TAGS, LOCATION, BIO, CONSENT, VERIFICATION = range(7)

# Database connection pool
db_pool = None

def init_db_pool():
    global db_pool
    try:
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            logger.error("DATABASE_URL environment variable not set.")
            raise ValueError("DATABASE_URL not set")
        url = urlparse(database_url)
        db_pool = psycopg2.pool.SimpleConnectionPool(
            1, 10,  # Min 1, max 10 connections
            database=url.path[1:],
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port
        )
        logger.info("Database connection pool initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize database pool: {e}")
        raise

def get_db_connection():
    try:
        return db_pool.getconn()
    except Exception as e:
        logger.error(f"Failed to get database connection: {e}")
        return None

def release_db_connection(conn):
    if conn:
        try:
            db_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Failed to release database connection: {e}")

def init_db():
    conn = get_db_connection()
    if not conn:
        logger.error("Could not get database connection for initialization.")
        return
    try:
        with conn.cursor() as c:
            c.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    profile JSONB,
                    consent BOOLEAN DEFAULT FALSE,
                    consent_time BIGINT,
                    premium_expiry BIGINT,
                    ban_type TEXT,
                    ban_expiry BIGINT,
                    verified BOOLEAN DEFAULT FALSE,
                    premium_features JSONB DEFAULT '{}'
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS reports (
                    report_id SERIAL PRIMARY KEY,
                    reporter_id BIGINT,
                    reported_id BIGINT,
                    timestamp BIGINT,
                    reason TEXT,
                    reporter_profile JSONB,
                    reported_profile JSONB
                )
            """)
            conn.commit()
            logger.info("Database tables initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        conn.rollback()
    finally:
        release_db_connection(conn)

try:
    init_db_pool()
    init_db()
except Exception as e:
    logger.error(f"Failed to set up database: {e}")
    exit(1)

def cleanup_in_memory(context: CallbackContext):
    current_time = time.time()
    command_timestamps.clear()
    for user_id in list(message_timestamps.keys()):
        message_timestamps[user_id] = [t for t in message_timestamps[user_id] if current_time - t < 60]
        if not message_timestamps[user_id]:
            del message_timestamps[user_id]
    for user_id in list(chat_histories.keys()):
        if not is_premium(user_id) and not has_premium_feature(user_id, "vaulted_chats"):
            del chat_histories[user_id]
    logger.debug("In-memory data cleaned up.")

def get_user(user_id: int) -> dict:
    conn = get_db_connection()
    if not conn:
        return {}
    try:
        with conn.cursor() as c:
            c.execute(
                "SELECT profile, consent, consent_time, premium_expiry, ban_type, ban_expiry, verified, premium_features "
                "FROM users WHERE user_id = %s",
                (user_id,)
            )
            result = c.fetchone()
            if result:
                return {
                    "profile": result[0] or {},
                    "consent": result[1],
                    "consent_time": result[2],
                    "premium_expiry": result[3],
                    "ban_type": result[4],
                    "ban_expiry": result[5],
                    "verified": result[6],
                    "premium_features": result[7] or {}
                }
            return {}
    except Exception as e:
        logger.error(f"Failed to get user {user_id}: {e}")
        return {}
    finally:
        release_db_connection(conn)

def update_user(user_id: int, data: dict):
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO users (user_id, profile, consent, consent_time, premium_expiry, ban_type, ban_expiry, verified, premium_features)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE
                SET profile = %s, consent = %s, consent_time = %s, premium_expiry = %s, ban_type = %s, ban_expiry = %s, verified = %s, premium_features = %s
            """, (
                user_id, json.dumps(data.get("profile", {})), data.get("consent", False), data.get("consent_time"),
                data.get("premium_expiry"), data.get("ban_type"), data.get("ban_expiry"), data.get("verified", False),
                json.dumps(data.get("premium_features", {})),
                json.dumps(data.get("profile", {})), data.get("consent", False), data.get("consent_time"),
                data.get("premium_expiry"), data.get("ban_type"), data.get("ban_expiry"), data.get("verified", False),
                json.dumps(data.get("premium_features", {}))
            ))
            conn.commit()
            logger.debug(f"Updated user {user_id} in database.")
    except Exception as e:
        logger.error(f"Failed to update user {user_id}: {e}")
        conn.rollback()
    finally:
        release_db_connection(conn)

def delete_user(user_id: int):
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor() as c:
            c.execute("DELETE FROM users WHERE user_id = %s", (user_id,))
            c.execute("DELETE FROM reports WHERE reporter_id = %s OR reported_id = %s", (user_id, user_id))
            conn.commit()
            logger.info(f"Deleted user {user_id} from database.")
    except Exception as e:
        logger.error(f"Failed to delete user {user_id}: {e}")
    finally:
        release_db_connection(conn)

def is_banned(user_id: int) -> bool:
    user = get_user(user_id)
    if user.get("ban_type"):
        if user["ban_type"] == "permanent" or (user["ban_type"] == "temporary" and user["ban_expiry"] > time.time()):
            return True
        if user["ban_type"] == "temporary" and user["ban_expiry"] <= time.time():
            update_user(user_id, {"ban_type": None, "ban_expiry": None})
    return False

def is_premium(user_id: int) -> bool:
    user = get_user(user_id)
    return user.get("premium_expiry") and user["premium_expiry"] > time.time()

def has_premium_feature(user_id: int, feature: str) -> bool:
    user = get_user(user_id)
    features = user.get("premium_features", {})
    return feature in features and (features[feature] is True or features[feature] > time.time())

def is_verified(user_id: int) -> bool:
    return get_user(user_id).get("verified", False)

def check_rate_limit(user_id: int, cooldown: int = COMMAND_COOLDOWN) -> bool:
    current_time = time.time()
    if user_id in command_timestamps and current_time - command_timestamps[user_id] < cooldown:
        return False
    command_timestamps[user_id] = current_time
    return True

def check_message_rate_limit(user_id: int) -> bool:
    current_time = time.time()
    message_timestamps[user_id] = [t for t in message_timestamps[user_id] if current_time - t < 60]
    max_messages = MAX_MESSAGES_PER_MINUTE + 10 if is_premium(user_id) or any(has_premium_feature(user_id, f) for f in ["shine_profile", "flare_messages"]) else MAX_MESSAGES_PER_MINUTE
    if len(message_timestamps[user_id]) >= max_messages:
        return False
    message_timestamps[user_id].append(current_time)
    return True

def is_safe_message(text: str) -> bool:
    if not text:
        return True
    text = text.lower()
    if any(word in text for word in BANNED_WORDS):
        return False
    url_pattern = re.compile(r'http[s]?://|www\.|\.com|\.org|\.net')
    if url_pattern.search(text):
        return False
    return True

def escape_markdown_v2(text: str) -> str:
    special_chars = r'_*\[\]()~`>#+\-=|{}.!'
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text

def safe_reply(update: Update, text: str, **kwargs) -> None:
    try:
        # Default to MarkdownV2 if parse_mode is not provided
        parse_mode = kwargs.pop("parse_mode", "MarkdownV2")
        if parse_mode == "MarkdownV2":
            text = escape_markdown_v2(text)  # Escape text for MarkdownV2
        if update.message:  # Handle Message updates (e.g., commands)
            update.message.reply_text(text, parse_mode=parse_mode, **kwargs)
        elif update.callback_query:  # Handle CallbackQuery updates (e.g., buttons)
            query = update.callback_query
            query.answer()  # Acknowledge the callback query
            query.message.reply_text(text, parse_mode=parse_mode, **kwargs)
        else:
            logger.error(f"No message or callback query found in update: {update}")
    except telegram.error.BadRequest as bre:
        logger.warning(f"MarkdownV2 parsing failed: {bre}. Sending without parse mode. Text: {text}")
        try:
            if update.callback_query:
                update.callback_query.message.reply_text(text, parse_mode=None, **kwargs)
            elif update.message:
                update.message.reply_text(text, parse_mode=None, **kwargs)
        except Exception as fallback_bre:
            logger.error(f"Failed to send fallback message without parse mode: {fallback_bre}")
    except Exception as e:
        logger.error(f"Failed to send message: {e}")
        try:
            error_text = "❌ An error occurred. Please try again."
            if update.callback_query:
                update.callback_query.message.reply_text(error_text, parse_mode=None)
            elif update.message:
                update.message.reply_text(error_text, parse_mode=None)
        except Exception as fallback_e:
            logger.error(f"Failed to send fallback error message: {fallback_e}")

def safe_bot_send_message(bot, chat_id: int, text: str, parse_mode: str = "MarkdownV2", **kwargs):
    """Safely send a message via bot, escaping MarkdownV2 if needed and falling back if parsing fails."""
    try:
        if parse_mode == "MarkdownV2":
            text = escape_markdown_v2(text)
        bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, **kwargs)
    except telegram.error.BadRequest as e:
        logger.warning(f"MarkdownV2 parsing failed for chat {chat_id}: {e}. Sending without parsing: {text}")
        bot.send_message(chat_id=chat_id, text=text, parse_mode=None, **kwargs)
    except Exception as e:
        logger.error(f"Failed to send message to {chat_id}: {e}")

def start(update: Update, context: CallbackContext) -> int:
    user_id = update.effective_user.id
    if is_banned(user_id):
        safe_reply(update, "🚫 You are currently banned. Contact support if you believe this is an error.", parse_mode="MarkdownV2")
        logger.info(f"Banned user {user_id} attempted to start a chat.")
        return ConversationHandler.END
    if not check_rate_limit(user_id):
        safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again.", parse_mode="MarkdownV2")
        return ConversationHandler.END
    if user_id in user_pairs:
        safe_reply(update, "💬 You're already in a chat. Use /next to switch or /stop to end.", parse_mode="MarkdownV2")
        return ConversationHandler.END
    user = get_user(user_id)
    if not user.get("consent"):
        keyboard = [
            [InlineKeyboardButton("✅ I Agree", callback_data="consent_agree")],
            [InlineKeyboardButton("❌ I Disagree", callback_data="consent_disagree")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        welcome_text = (
            "🌟 *Welcome to Talk2Anyone!* 🌟\n\n"
            "Chat anonymously with people worldwide! 🌍\n"
            "Here are the rules to keep it fun and safe:\n"
            "🚫 No harassment, spam, or inappropriate content\n"
            "🤝 Respect everyone at all times\n"
            "📢 Report issues with /report\n"
            "⚠️ Violations may lead to bans\n\n"
            "🔒 *Privacy*: We only store your user ID, profile, and consent securely. Use /deleteprofile to remove your data.\n\n"
            "Do you agree to the rules?"
        )
        safe_reply(update, welcome_text, parse_mode="MarkdownV2", reply_markup=reply_markup)
        return CONSENT
    if not user.get("verified"):
        safe_reply(update, "🔐 Please verify your profile to start chatting. Enter a short verification phrase (e.g., 'I’m here to chat respectfully'):", parse_mode="MarkdownV2")
        return VERIFICATION
    if user_id not in waiting_users:
        if is_premium(user_id) or has_premium_feature(user_id, "shine_profile"):
            waiting_users.insert(0, user_id)
        else:
            waiting_users.append(user_id)
        safe_reply(update, "🔍 Looking for a chat partner... Please wait!", parse_mode="MarkdownV2")
    match_users(context)
    return ConversationHandler.END

def consent_handler(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    choice = query.data
    if choice == "consent_agree":
        update_user(user_id, {
            "consent": True,
            "consent_time": int(time.time()),
            "profile": get_user(user_id).get("profile", {})
        })
        try:
            query.message.edit_text(escape_markdown_v2("✅ Thank you for agreeing! Let’s verify your profile next."), parse_mode="MarkdownV2")
            query.message.reply_text(escape_markdown_v2("✍️ Enter a short verification phrase (e.g., 'I’m here to chat respectfully'):"), parse_mode="MarkdownV2")
        except telegram.error.BadRequest as e:
            logger.warning(f"Failed to edit consent message: {e}")
            query.message.reply_text("✅ Thank you for agreeing! Let’s verify your profile next.", parse_mode=None)
            query.message.reply_text("✍️ Enter a short verification phrase (e.g., 'I’m here to chat respectfully'):", parse_mode=None)
        return VERIFICATION
    else:
        try:
            query.message.edit_text(escape_markdown_v2("❌ You must agree to the rules to use this bot. Use /start to try again."), parse_mode="MarkdownV2")
        except telegram.error.BadRequest as e:
            logger.warning(f"Failed to edit consent disagree message: {e}")
            query.message.reply_text("❌ You must agree to the rules to use this bot. Use /start to try again.", parse_mode=None)
        logger.info(f"User {user_id} declined rules.")
        return ConversationHandler.END

def verification_handler(update: Update, context: CallbackContext) -> int:
    user_id = update.effective_user.id
    phrase = update.message.text.strip()
    if len(phrase) < 10 or not is_safe_message(phrase):
        safe_reply(update, "⚠️ Please provide a valid, respectful verification phrase (min 10 characters).", parse_mode="MarkdownV2")
        return VERIFICATION
    user = get_user(user_id)
    update_user(user_id, {
        "verified": True,
        "profile": user.get("profile", {}),
        "consent": user.get("consent", False)
    })
    safe_reply(update, "🎉 Profile verified successfully! Let’s get started.", parse_mode="MarkdownV2")
    if user_id not in waiting_users:
        if is_premium(user_id) or has_premium_feature(user_id, "shine_profile"):
            waiting_users.insert(0, user_id)
        else:
            waiting_users.append(user_id)
        safe_reply(update, "🔍 Looking for a chat partner... Please wait!", parse_mode="MarkdownV2")
    match_users(context)
    return ConversationHandler.END

def match_users(context: CallbackContext) -> None:
    if len(waiting_users) < 2:
        return
    i = 0
    while i < len(waiting_users):
        user1 = waiting_users[i]
        j = i + 1
        while j < len(waiting_users):
            user2 = waiting_users[j]
            if can_match(user1, user2):
                waiting_users.remove(user1)
                waiting_users.remove(user2)
                user_pairs[user1] = user2
                user_pairs[user2] = user1
                previous_partners[user1] = user2
                previous_partners[user2] = user1
                safe_bot_send_message(context.bot, user1, "✅ *Connected!* Start chatting now! 🗣️\nUse /help for more options.", parse_mode="MarkdownV2")
                safe_bot_send_message(context.bot, user2, "✅ *Connected!* Start chatting now! 🗣️\nUse /help for more options.", parse_mode="MarkdownV2")
                logger.info(f"Matched users {user1} and {user2}.")
                if is_premium(user1) or has_premium_feature(user1, "vaulted_chats"):
                    chat_histories[user1] = []
                if is_premium(user2) or has_premium_feature(user2, "vaulted_chats"):
                    chat_histories[user2] = []
                return
            j += 1
        i += 1

def can_match(user1: int, user2: int) -> bool:
    profile1 = get_user(user1).get("profile", {})
    profile2 = get_user(user2).get("profile", {})
    if not profile1 or not profile2:
        return True
    tags1 = set(profile1.get("tags", []))
    tags2 = set(profile2.get("tags", []))
    if tags1 and tags2 and not tags1.intersection(tags2):
        return False
    age1 = profile1.get("age")
    age2 = profile2.get("age")
    if age1 and age2 and abs(age1 - age2) > (15 if is_premium(user1) or is_premium(user2) or has_premium_feature(user1, "mood_match") or has_premium_feature(user2, "mood_match") else 10):
        return False
    gender_pref1 = profile1.get("gender_preference")
    gender_pref2 = profile2.get("gender_preference")
    gender1 = profile1.get("gender")
    gender2 = profile2.get("gender")
    if gender_pref1 and gender2 and gender_pref1 != gender2:
        return False
    if gender_pref2 and gender1 and gender_pref2 != gender1:
        return False
    if has_premium_feature(user1, "mood_match") and has_premium_feature(user2, "mood_match"):
        mood1 = profile1.get("mood")
        mood2 = profile2.get("mood")
        if mood1 and mood2 and mood1 != mood2:
            return False
    return True

def stop(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        safe_reply(update, "🚫 You are currently banned.", parse_mode="MarkdownV2")
        return
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        del user_pairs[user_id]
        if partner_id in user_pairs:
            del user_pairs[partner_id]
        safe_bot_send_message(context.bot, partner_id, "👋 Your partner has left the chat. Use /start to find a new one.", parse_mode="MarkdownV2")
        safe_reply(update, "👋 Chat ended. Use /start to begin a new chat.", parse_mode="MarkdownV2")
        logger.info(f"User {user_id} stopped chat with {partner_id}.")
        if user_id in chat_histories and not has_premium_feature(user_id, "vaulted_chats"):
            del chat_histories[user_id]
    else:
        safe_reply(update, "❓ You're not in a chat. Use /start to find a partner.", parse_mode="MarkdownV2")

def next_chat(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        safe_reply(update, "🚫 You are banned from using this bot.")
        return
    if not check_rate_limit(user_id):
        safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again.")
        return
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        del user_pairs[user_id]
        del user_pairs[partner_id]
        safe_bot_send_message(context.bot, partner_id, "🔌 Your chat partner disconnected.")
    if user_id not in waiting_users:  # Prevent duplicates
        waiting_users.append(user_id)
    safe_reply(update, "🔍 Looking for a new chat partner...")
    match_users(context.bot)
    logger.info(f"User {user_id} requested next chat.")


def help_command(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        safe_reply(update, "🚫 You are currently banned.", parse_mode="MarkdownV2")
        return
    keyboard = [
        [InlineKeyboardButton("💬 Start Chat", callback_data="start_chat"),
         InlineKeyboardButton("🔍 Next Partner", callback_data="next_chat")],
        [InlineKeyboardButton("👋 Stop Chat", callback_data="stop_chat"),
         InlineKeyboardButton("⚙️ Settings", callback_data="settings_menu")],
        [InlineKeyboardButton("🌟 Premium", callback_data="premium_menu"),
         InlineKeyboardButton("📜 History", callback_data="history_menu")],
        [InlineKeyboardButton("🚨 Report", callback_data="report_user"),
         InlineKeyboardButton("🔄 Re-Match", callback_data="rematch_partner")],
        [InlineKeyboardButton("🗑️ Delete Profile", callback_data="delete_profile")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    help_text = (
        "🌟 *Talk2Anyone Help Menu* 🌟\n\n"
        "Here’s how you can use the bot:\n"
        "💬 *Chat Commands*\n"
        "• /start - Begin a new anonymous chat\n"
        "• /next - Find a new chat partner\n"
        "• /stop - End the current chat\n"
        "⚙️ *Profile & Settings*\n"
        "• /settings - Customize your profile\n"
        "• /deleteprofile - Erase your data\n"
        "🌟 *Premium Features*\n"
        "• /premium - Unlock amazing features\n"
        "• /history - View past chats [Premium]\n"
        "• /rematch - Reconnect with past partners [Premium]\n"
        "• /shine - Boost your profile [Premium]\n"
        "• /instant - Instant rematch [Premium]\n"
        "• /mood - Set chat mood [Premium]\n"
        "• /vault - Save chats forever [Premium]\n"
        "• /flare - Add sparkle to messages [Premium]\n"
        "🚨 *Safety*\n"
        "• /report - Report inappropriate behavior\n"
        "Use the buttons below to explore! 👇"
    )
    safe_reply(update, help_text, parse_mode="MarkdownV2", reply_markup=reply_markup)

def premium(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        safe_reply(update, "🚫 You are currently banned.", parse_mode="MarkdownV2")
        return
    keyboard = [
        [InlineKeyboardButton("✨ Flare Messages - 100 ⭐", callback_data="buy_flare"),
         InlineKeyboardButton("🔄 Instant Rematch - 100 ⭐", callback_data="buy_instant")],
        [InlineKeyboardButton("🌟 Shine Profile - 250 ⭐", callback_data="buy_shine"),
         InlineKeyboardButton("😊 Mood Match - 250 ⭐", callback_data="buy_mood")],
        [InlineKeyboardButton("📜 Vaulted Chats - 500 ⭐", callback_data="buy_vault")],
        [InlineKeyboardButton("🎉 Premium Pass - 1000 ⭐", callback_data="buy_premium_pass")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    message_text = (
        "🌟 *Unlock Premium Features!* 🌟\n\n"
        "Enhance your chat experience with these amazing perks:\n"
        "✨ *Flare Messages* - Add sparkle effects for 7 days (100 ⭐)\n"
        "🔄 *Instant Rematch* - Reconnect anytime (100 ⭐)\n"
        "🌟 *Shine Profile* - Priority matching for 24 hours (250 ⭐)\n"
        "😊 *Mood Match* - Vibe-based matches for 30 days (250 ⭐)\n"
        "📜 *Vaulted Chats* - Save chats forever (500 ⭐)\n"
        "🎉 *Premium Pass* - All features for 30 days + 5 Instant Rematches (1000 ⭐)\n\n"
        "👇 Tap a button to purchase with Telegram Stars!"
    )
    safe_reply(update, message_text, parse_mode="MarkdownV2", reply_markup=reply_markup)

def buy_premium(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    if is_banned(user_id):
        try:
            query.message.edit_text(escape_markdown_v2("🚫 You are currently banned."), parse_mode="MarkdownV2")
        except telegram.error.BadRequest:
            query.message.reply_text("🚫 You are currently banned.", parse_mode=None)
        return
    feature_map = {
        "buy_flare": ("Flare Messages", 100, "Add sparkle effects to your messages for 7 days!", "flare_messages"),
        "buy_instant": ("Instant Rematch", 100, "Reconnect with any past partner instantly!", "instant_rematch"),
        "buy_shine": ("Shine Profile", 250, "Boost your profile to the top for 24 hours!", "shine_profile"),
        "buy_mood": ("Mood Match", 250, "Match with users sharing your vibe for 30 days!", "mood_match"),
        "buy_vault": ("Vaulted Chats", 500, "Save your chats forever!", "vaulted_chats"),
        "buy_premium_pass": ("Premium Pass", 1000, "Unlock Shine Profile, Mood Match, Vaulted Chats, Flare Messages (30 days), and 5 Instant Rematches!", "premium_pass"),
    }
    choice = query.data
    if choice not in feature_map:
        try:
            query.message.edit_text(escape_markdown_v2("❌ Invalid feature selected."), parse_mode="MarkdownV2")
        except telegram.error.BadRequest:
            query.message.reply_text("❌ Invalid feature selected.", parse_mode=None)
        return
    title, stars, desc, feature_key = feature_map[choice]
    try:
        context.bot.send_invoice(
            chat_id=user_id,
            title=title,
            description=desc,
            payload=f"{feature_key}_{user_id}",
            currency="XTR",
            prices=[LabeledPrice(title, stars)],
            start_parameter=f"buy-{feature_key}",
            provider_token=None
        )
        logger.info(f"Sent Stars invoice for user {user_id}: {title} ({stars} Stars)")
    except Exception as e:
        logger.error(f"Failed to send invoice for user {user_id}: {e}")
        try:
            query.message.edit_text(escape_markdown_v2("❌ Error generating payment invoice. Please try again."), parse_mode="MarkdownV2")
        except telegram.error.BadRequest:
            query.message.reply_text("❌ Error generating payment invoice. Please try again.", parse_mode=None)

def pre_checkout(update: Update, context: CallbackContext) -> None:
    query = update.pre_checkout_query
    if query.currency != "XTR":
        context.bot.answer_pre_checkout_query(
            query.id, ok=False, error_message="Only Telegram Stars payments are supported."
        )
        return
    valid_payloads = [f"{key}_{query.from_user.id}" for key in ["flare_messages", "instant_rematch", "shine_profile", "mood_match", "vaulted_chats", "premium_pass"]]
    if query.invoice_payload in valid_payloads:
        context.bot.answer_pre_checkout_query(query.id, ok=True)
        logger.info(f"Approved pre-checkout for user {query.from_user.id}: {query.invoice_payload}")
    else:
        context.bot.answer_pre_checkout_query(
            query.id, ok=False, error_message="Invalid purchase payload."
        )
        logger.warning(f"Rejected pre-checkout for user {query.from_user.id}: {query.invoice_payload}")

def successful_payment(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    payment = update.message.successful_payment
    if payment.currency != "XTR":
        logger.warning(f"Non-Stars payment received from user {user_id}: {payment.currency}")
        return
    payload = payment.invoice_payload
    current_time = int(time.time())
    feature_map = {
        "flare_messages": (7 * 24 * 3600, "✨ Flare Messages activated for 7 days! Your messages will now sparkle!"),
        "instant_rematch": (None, "🔄 Instant Rematch unlocked! Use /instant to reconnect."),
        "shine_profile": (24 * 3600, "🌟 Shine Profile activated for 24 hours! You’re now at the top of the match list!"),
        "mood_match": (30 * 24 * 3600, "😊 Mood Match activated for 30 days! Find users with similar vibes!"),
        "vaulted_chats": (None, "📜 Vaulted Chats unlocked forever! Save your chats with /vault!"),
        "premium_pass": (None, "🎉 Premium Pass activated! Enjoy all features for 30 days + 5 Instant Rematches!"),
    }
    user = get_user(user_id)
    features = user.get("premium_features", {})
    for feature, (duration, message) in feature_map.items():
        if payload.startswith(feature):
            if feature == "premium_pass":
                features["shine_profile"] = current_time + 30 * 24 * 3600
                features["mood_match"] = current_time + 30 * 24 * 3600
                features["vaulted_chats"] = True
                features["flare_messages"] = current_time + 30 * 24 * 3600
                features["instant_rematch_count"] = features.get("instant_rematch_count", 0) + 5
            else:
                expiry = current_time + duration if duration else True
                if feature == "instant_rematch":
                    features["instant_rematch_count"] = features.get("instant_rematch_count", 0) + 1
                else:
                    features[feature] = expiry
            update_user(user_id, {"premium_features": features})
            safe_reply(update, message, parse_mode="MarkdownV2")
            logger.info(f"User {user_id} purchased {feature} with Stars")
            break
    else:
        logger.warning(f"Unknown payload for user {user_id}: {payload}")
        safe_reply(update, "❌ Unknown purchase error. Please contact support.", parse_mode="MarkdownV2")

def shine(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        safe_reply(update, "🚫 You are currently banned.", parse_mode="MarkdownV2")
        return
    if not has_premium_feature(user_id, "shine_profile"):
        safe_reply(update, "🌟 Shine Profile is a premium feature. Buy it with /premium!", parse_mode="MarkdownV2")
        return
    if user_id not in waiting_users and user_id not in user_pairs:
        waiting_users.insert(0, user_id)
        safe_reply(update, "✨ Your profile is now shining! You’re first in line for matches!", parse_mode="MarkdownV2")
        match_users(context)
    else:
        safe_reply(update, "❓ You're already in a chat or waiting list.", parse_mode="MarkdownV2")

def instant(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        safe_reply(update, "🚫 You are currently banned.", parse_mode="MarkdownV2")
        return
    user = get_user(user_id)
    features = user.get("premium_features", {})
    rematch_count = features.get("instant_rematch_count", 0)
    if rematch_count <= 0:
        safe_reply(update, "🔄 You need an Instant Rematch! Buy one with /premium!", parse_mode="MarkdownV2")
        return
    partners = user.get("profile", {}).get("past_partners", [])
    if not partners:
        safe_reply(update, "❌ No past partners to rematch with.", parse_mode="MarkdownV2")
        return
    partner_id = partners[-1]
    if partner_id in user_pairs:
        safe_reply(update, "❌ Your previous partner is currently in another chat.", parse_mode="MarkdownV2")
        return
    if partner_id in waiting_users:
        waiting_users.remove(partner_id)
    user_pairs[user_id] = partner_id
    user_pairs[partner_id] = user_id
    features["instant_rematch_count"] = rematch_count - 1
    update_user(user_id, {"premium_features": features})
    safe_reply(update, "🔄 Instantly reconnected! Start chatting! 🗣️", parse_mode="MarkdownV2")
    safe_bot_send_message(context.bot, partner_id, "🔄 Instantly reconnected! Start chatting! 🗣️", parse_mode="MarkdownV2")
    logger.info(f"User {user_id} used Instant Rematch with {partner_id}")

def mood(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        safe_reply(update, "🚫 You are currently banned.", parse_mode="MarkdownV2")
        return
    if not has_premium_feature(user_id, "mood_match"):
        safe_reply(update, "😊 Mood Match is a premium feature. Buy it with /premium!", parse_mode="MarkdownV2")
        return
    keyboard = [
        [InlineKeyboardButton("😎 Chill", callback_data="mood_chill"),
         InlineKeyboardButton("🤔 Deep", callback_data="mood_deep")],
        [InlineKeyboardButton("😂 Fun", callback_data="mood_fun"),
         InlineKeyboardButton("❌ Clear Mood", callback_data="mood_clear")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    safe_reply(update, "🎭 Choose your chat mood:", parse_mode="MarkdownV2", reply_markup=reply_markup)

def set_mood(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    if not has_premium_feature(user_id, "mood_match"):
        try:
            query.message.edit_text(escape_markdown_v2("😊 Mood Match is a premium feature. Buy it with /premium!"), parse_mode="MarkdownV2")
        except telegram.error.BadRequest:
            query.message.reply_text("😊 Mood Match is a premium feature. Buy it with /premium!", parse_mode=None)
        return
    choice = query.data
    user = get_user(user_id)
    profile = user.get("profile", {})
    if choice == "mood_clear":
        profile.pop("mood", None)
        try:
            query.message.edit_text(escape_markdown_v2("❌ Mood cleared successfully."), parse_mode="MarkdownV2")
        except telegram.error.BadRequest:
            query.message.reply_text("❌ Mood cleared successfully.", parse_mode=None)
    else:
        mood = choice.split("_")[1]
        profile["mood"] = mood
        try:
            query.message.edit_text(escape_markdown_v2(f"🎭 Mood set to: *{mood.capitalize()}*!"), parse_mode="MarkdownV2")
        except telegram.error.BadRequest:
            query.message.reply_text(f"🎭 Mood set to: {mood.capitalize()}!", parse_mode=None)
    update_user(user_id, {"profile": profile})

def vault(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        safe_reply(update, "🚫 You are currently banned.", parse_mode="MarkdownV2")
        return
    if not has_premium_feature(user_id, "vaulted_chats"):
        safe_reply(update, "📜 Vaulted Chats is a premium feature. Buy it with /premium!", parse_mode="MarkdownV2")
        return
    if user_id not in chat_histories or not chat_histories[user_id]:
        safe_reply(update, "❌ No chats saved in your vault.", parse_mode="MarkdownV2")
        return
    history_text = "📜 *Your Vaulted Chats* 📜\n\n"
    for msg in chat_histories[user_id][-10:]:
        history_text += f"[{msg['time']}]: {escape_markdown_v2(msg['text'])}\n"
    safe_reply(update, history_text, parse_mode="MarkdownV2")

def flare(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        safe_reply(update, "🚫 You are currently banned.", parse_mode="MarkdownV2")
        return
    if not has_premium_feature(user_id, "flare_messages"):
        safe_reply(update, "✨ Flare Messages is a premium feature. Buy it with /premium!", parse_mode="MarkdownV2")
        return
    user = get_user(user_id)
    features = user.get("premium_features", {})
    flare_active = features.get("flare_active", False)
    features["flare_active"] = not flare_active
    update_user(user_id, {"premium_features": features})
    safe_reply(update, f"✨ Flare Messages *{'enabled' if not flare_active else 'disabled'}*!", parse_mode="MarkdownV2")

def history(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        safe_reply(update, "🚫 You are currently banned.", parse_mode="MarkdownV2")
        return
    if not (is_premium(user_id) or has_premium_feature(user_id, "vaulted_chats")):
        safe_reply(update, "📜 Chat history is a premium feature. Use /premium to unlock!", parse_mode="MarkdownV2")
        return
    if user_id not in chat_histories or not chat_histories[user_id]:
        safe_reply(update, "❌ No chat history available.", parse_mode="MarkdownV2")
        return
    history_text = "📜 *Your Chat History* 📜\n\n"
    for msg in chat_histories[user_id][-10:]:
        history_text += f"[{msg['time']}]: {escape_markdown_v2(msg['text'])}\n"
    safe_reply(update, history_text, parse_mode="MarkdownV2")

def report(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        safe_reply(update, "🚫 You are currently banned.", parse_mode="MarkdownV2")
        return
    if not check_rate_limit(user_id):
        safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again.", parse_mode="MarkdownV2")
        return
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        conn = get_db_connection()
        if not conn:
            safe_reply(update, "❌ Error processing report due to database issue.", parse_mode="MarkdownV2")
            return
        try:
            with conn.cursor() as c:
                reason = " ".join(context.args) if context.args else "No reason provided"
                c.execute("""
                    INSERT INTO reports (reporter_id, reported_id, timestamp, reason, reporter_profile, reported_profile)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    user_id, partner_id, int(time.time()), reason,
                    json.dumps(get_user(user_id).get("profile", {})),
                    json.dumps(get_user(partner_id).get("profile", {}))
                ))
                conn.commit()
                c.execute("SELECT COUNT(*) FROM reports WHERE reported_id = %s", (partner_id,))
                report_count = c.fetchone()[0]
                if report_count >= REPORT_THRESHOLD:
                    update_user(partner_id, {
                        "ban_type": "temporary",
                        "ban_expiry": int(time.time()) + TEMP_BAN_DURATION,
                        "profile": get_user(partner_id).get("profile", {}),
                        "consent": get_user(partner_id).get("consent", False),
                        "verified": get_user(partner_id).get("verified", False)
                    })
                    safe_bot_send_message(context.bot, partner_id, "⚠️ You’ve been temporarily banned due to multiple reports.", parse_mode="MarkdownV2")
                    logger.warning(f"User {partner_id} banned temporarily due to {report_count} reports.")
                    stop(update, context)
                safe_reply(update, "✅ Thank you for reporting. We’ll review it. Use /next to find a new partner.", parse_mode="MarkdownV2")
                logger.info(f"User {user_id} reported user {partner_id}. Reason: {reason}. Total reports: {report_count}.")
        except Exception as e:
            logger.error(f"Failed to log report: {e}")
            safe_reply(update, "❌ Error processing report.", parse_mode="MarkdownV2")
        finally:
            release_db_connection(conn)
    else:
        safe_reply(update, "❓ You're not in a chat. Use /start to begin.", parse_mode="MarkdownV2")

def handle_message(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        safe_reply(update, "🚫 You are currently banned.", parse_mode="MarkdownV2")
        return
    if not check_message_rate_limit(user_id):
        safe_reply(update, "⏳ You're sending messages too fast. Please slow down.", parse_mode="MarkdownV2")
        logger.info(f"User {user_id} hit message rate limit.")
        return
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        message_text = update.message.text
        if not is_safe_message(message_text):
            safe_reply(update, "⚠️ Inappropriate content detected. Please keep the chat respectful.", parse_mode="MarkdownV2")
            logger.info(f"User {user_id} sent unsafe message: {message_text}")
            return
        flare = has_premium_feature(user_id, "flare_messages") and get_user(user_id).get("premium_features", {}).get("flare_active", False)
        final_text = f"✨ {escape_markdown_v2(message_text)} ✨" if flare else escape_markdown_v2(message_text)
        safe_bot_send_message(context.bot, partner_id, final_text, parse_mode="MarkdownV2")
        logger.info(f"Message from {user_id} to {partner_id}: {message_text}")
        if is_premium(user_id) or has_premium_feature(user_id, "vaulted_chats"):
            if user_id in chat_histories:
                chat_histories[user_id].append({
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "text": f"You: {message_text}"
                })
        if is_premium(partner_id) or has_premium_feature(partner_id, "vaulted_chats"):
            if partner_id in chat_histories:
                chat_histories[partner_id].append({
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "text": f"Partner: {message_text}"
                })

def settings(update: Update, context: CallbackContext) -> int:
    user_id = update.effective_user.id
    if is_banned(user_id):
        safe_reply(update, "🚫 You are currently banned.", parse_mode="MarkdownV2")
        return ConversationHandler.END
    if not check_rate_limit(user_id):
        safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again.", parse_mode="MarkdownV2")
        return ConversationHandler.END
    user = get_user(user_id)
    profile = user.get("profile", {})
    keyboard = [
        [
            InlineKeyboardButton(f"👤 Gender: {profile.get('gender', 'Not set')}", callback_data="set_gender"),
            InlineKeyboardButton(f"🎂 Age: {profile.get('age', 'Not set')}", callback_data="set_age")
        ],
        [
            InlineKeyboardButton(f"🏷️ Tags: {', '.join(profile.get('tags', [])) or 'Not set'}", callback_data="set_tags"),
            InlineKeyboardButton(f"📍 Location: {profile.get('location', 'Not set')}", callback_data="set_location")
        ],
        [
            InlineKeyboardButton(f"📝 Bio: {profile.get('bio', 'Not set')[:20] + '...' if profile.get('bio') else 'Not set'}", callback_data="set_bio"),
            InlineKeyboardButton("❤️ Gender Preference", callback_data="set_gender_pref")
        ],
        [InlineKeyboardButton("🔙 Back to Chat", callback_data="back_to_chat")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    settings_text = (
        "⚙️ *Your Profile Settings* ⚙️\n\n"
        "Customize your profile below. Tap an option to edit it!"
    )
    safe_reply(update, settings_text, parse_mode="MarkdownV2", reply_markup=reply_markup)
    return GENDER

def button(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    if not query:
        logger.error("No callback query found in update.")
        return ConversationHandler.END

    user_id = query.from_user.id
    data = query.data

    try:
        query.answer()  # Acknowledge the callback query

        # Check for bans
        if is_banned(user_id):
            try:
                query.message.reply_text(escape_markdown_v2("🚫 You are banned from using this bot."), parse_mode="MarkdownV2")
            except telegram.error.BadRequest:
                logger.warning(f"Failed to send ban message to user {user_id}.")
            return ConversationHandler.END

        # Main menu buttons
        if data == "start_chat":
            start(update, context)
            return ConversationHandler.END
        elif data == "next_chat":
            next_chat(update, context)
            return ConversationHandler.END
        elif data == "stop_chat":
            stop(update, context)
            return ConversationHandler.END
        elif data == "settings_menu":
            return settings(update, context)
        elif data == "premium_menu":
            premium(update, context)
            return ConversationHandler.END
        elif data == "history_menu":
            history(update, context)
            return ConversationHandler.END
        elif data == "report_user":
            report(update, context)
            return ConversationHandler.END
        elif data == "rematch_partner":
            rematch(update, context)
            return ConversationHandler.END
        elif data == "delete_profile":
            delete_profile(update, context)
            return ConversationHandler.END

        # Settings menu buttons
        elif data == "set_gender":
            keyboard = [
                [
                    InlineKeyboardButton("👨 Male", callback_data="gender_male"),
                    InlineKeyboardButton("👩 Female", callback_data="gender_female"),
                    InlineKeyboardButton("🌈 Other", callback_data="gender_other")
                ],
                [
                    InlineKeyboardButton("➖ Skip", callback_data="gender_skip"),
                    InlineKeyboardButton("🔙 Back", callback_data="back_to_settings")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            try:
                query.message.edit_text(
                    escape_markdown_v2("👤 *Set Your Gender* 👤\n\nChoose your gender below:"),
                    parse_mode="MarkdownV2",
                    reply_markup=reply_markup
                )
            except telegram.error.BadRequest:
                query.message.reply_text(
                    "👤 Set Your Gender 👤\n\nChoose your gender below:",
                    parse_mode="MarkdownV2",
                    reply_markup=reply_markup
                )
            return GENDER
        elif data == "set_age":
            try:
                query.message.edit_text(
                    escape_markdown_v2("🎂 *Set Your Age* 🎂\n\nPlease enter your age (e.g., 25):"),
                    parse_mode="MarkdownV2"
                )
            except telegram.error.BadRequest:
                query.message.reply_text(
                    "🎂 Set Your Age 🎂\n\nPlease enter your age (e.g., 25):",
                    parse_mode="MarkdownV2"
                )
            return AGE
        elif data == "set_tags":
            try:
                query.message.edit_text(
                    escape_markdown_v2(
                        "🏷️ *Set Your Tags* 🏷️\n\n"
                        f"Enter tags to match with others (comma-separated, e.g., music, gaming).\n"
                        f"Available tags: {', '.join(ALLOWED_TAGS)}"
                    ),
                    parse_mode="MarkdownV2"
                )
            except telegram.error.BadRequest:
                query.message.reply_text(
                    escape_markdown_v2(
                        "🏷️ *Set Your Tags* 🏷️\n\n"
                        f"Enter tags to match with others (comma-separated, e.g., music, gaming).\n"
                        f"Available tags: {', '.join(ALLOWED_TAGS)}"
                    ),
                    parse_mode="MarkdownV2"
                )
            return TAGS
        elif data == "set_location":
            try:
                query.message.edit_text(
                    escape_markdown_v2("📍 *Set Your Location* 📍\n\nEnter your location (e.g., New York):"),
                    parse_mode="MarkdownV2"
                )
            except telegram.error.BadRequest:
                query.message.reply_text(
                    "📍 Set Your Location 📍\n\nEnter your location (e.g., New York):",
                    parse_mode="MarkdownV2"
                )
            return LOCATION
        elif data == "set_bio":
            try:
                query.message.edit_text(
                    escape_markdown_v2(
                        "📝 *Set Your Bio* 📝\n\n"
                        f"Enter a short bio (max {MAX_PROFILE_LENGTH} characters):"
                    ),
                    parse_mode="MarkdownV2"
                )
            except telegram.error.BadRequest:
                query.message.reply_text(
                    escape_markdown_v2(
                        "📝 *Set Your Bio* 📝\n\n"
                        f"Enter a short bio (max {MAX_PROFILE_LENGTH} characters):"
                    ),
                    parse_mode="MarkdownV2"
                )
            return BIO
        elif data == "set_gender_pref":
            keyboard = [
                [
                    InlineKeyboardButton("👨 Male", callback_data="pref_male"),
                    InlineKeyboardButton("👩 Female", callback_data="pref_female"),
                    InlineKeyboardButton("🌈 Any", callback_data="pref_any")
                ],
                [InlineKeyboardButton("🔙 Back", callback_data="back_to_settings")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            try:
                query.message.edit_text(
                    escape_markdown_v2("❤️ *Set Gender Preference* ❤️\n\nSelect preferred gender to match with:"),
                    parse_mode="MarkdownV2",
                    reply_markup=reply_markup
                )
            except telegram.error.BadRequest:
                query.message.reply_text(
                    "❤️ Set Gender Preference ❤️\n\nSelect preferred gender to match with:",
                    parse_mode="MarkdownV2",
                    reply_markup=reply_markup
                )
            return GENDER
        elif data == "back_to_settings":
            return settings(update, context)
        elif data == "back_to_chat":
            try:
                query.message.edit_text(
                    escape_markdown_v2("👋 Returning to chat! Use /settings to edit your profile again."),
                    parse_mode="MarkdownV2"
                )
            except telegram.error.BadRequest:
                query.message.reply_text(
                    "👋 Returning to chat! Use /settings to edit your profile again.",
                    parse_mode="MarkdownV2"
                )
            return ConversationHandler.END

        # Gender and preference selections
        elif data.startswith("gender_") or data.startswith("pref_"):
            user = get_user(user_id)
            profile = user.get("profile", {})
            if data.startswith("gender_"):
                if data == "gender_skip":
                    try:
                        query.message.edit_text(
                            escape_markdown_v2("👤 Gender skipped."),
                            parse_mode="MarkdownV2"
                        )
                    except telegram.error.BadRequest:
                        query.message.reply_text("👤 Gender skipped.", parse_mode="MarkdownV2")
                else:
                    gender = data.split("_")[1].capitalize()
                    profile["gender"] = gender
                    try:
                        query.message.edit_text(
                            escape_markdown_v2(f"👤 Gender set to: *{gender}*! 🎉"),
                            parse_mode="MarkdownV2"
                        )
                    except telegram.error.BadRequest:
                        query.message.reply_text(f"👤 Gender set to: {gender}! 🎉", parse_mode="MarkdownV2")
            elif data.startswith("pref_"):
                pref = data.split("_")[1].capitalize()
                if pref == "Any":
                    profile.pop("gender_preference", None)
                else:
                    profile["gender_preference"] = pref
                try:
                    query.message.edit_text(
                        escape_markdown_v2(f"❤️ Gender preference set to: *{pref}*! 🎉"),
                        parse_mode="MarkdownV2"
                    )
                except telegram.error.BadRequest:
                    query.message.reply_text(f"❤️ Gender preference set to: {pref}! 🎉", parse_mode="MarkdownV2")
            update_user(user_id, {"profile": profile, "consent": user.get("consent", False), "verified": user.get("verified", False)})
            return settings(update, context)

        else:
            try:
                query.message.reply_text(
                    escape_markdown_v2("❌ Unknown action. Please try again."),
                    parse_mode="MarkdownV2"
                )
            except telegram.error.BadRequest:
                logger.warning(f"Failed to send unknown action message to user {user_id}.")
            return ConversationHandler.END

    except Exception as e:
        logger.error(f"Error in button handler for data '{data}' by user {user_id}: {e}", exc_info=True)
        try:
            query.message.reply_text(
                escape_markdown_v2("❌ An error occurred. Please try again."),
                parse_mode="MarkdownV2"
            )
        except telegram.error.BadRequest:
            logger.warning(f"Failed to send error message to user {user_id}.")
        return ConversationHandler.END

def set_age(update: Update, context: CallbackContext) -> int:
    user_id = update.effective_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    age_text = update.message.text.strip()
    try:
        age = int(age_text)
        if age < 13 or age > 120:
            safe_reply(update, "⚠️ Age must be between 13 and 120. Please try again.", parse_mode="MarkdownV2")
            return AGE
        profile["age"] = age
        update_user(user_id, {"profile": profile, "consent": user.get("consent", False), "verified": user.get("verified", False)})
        safe_reply(update, f"🎂 Age set to: *{age}*! 🎉", parse_mode="MarkdownV2")
        return settings(update, context)
    except ValueError:
        safe_reply(update, "⚠️ Please enter a valid number for your age (e.g., 25).", parse_mode="MarkdownV2")
        return AGE

def set_tags(update: Update, context: CallbackContext) -> int:
    user_id = update.effective_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    tags = [tag.strip().lower() for tag in update.message.text.split(",") if tag.strip().lower() in ALLOWED_TAGS]
    if not tags and update.message.text.strip():
        safe_reply(update, f"⚠️ Invalid tags. Choose from: *{', '.join(ALLOWED_TAGS)}*", parse_mode="MarkdownV2")
        return TAGS
    profile["tags"] = tags
    update_user(user_id, {"profile": profile, "consent": user.get("consent", False), "verified": user.get("verified", False)})
    safe_reply(update, f"🏷️ Tags set to: *{', '.join(tags) if tags else 'None'}*! 🎉", parse_mode="MarkdownV2")
    return settings(update, context)

def set_location(update: Update, context: CallbackContext) -> int:
    user_id = update.effective_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    location = update.message.text.strip()
    if len(location) > 100:
        safe_reply(update, "⚠️ Location must be under 100 characters.", parse_mode="MarkdownV2")
        return LOCATION
    profile["location"] = location
    update_user(user_id, {"profile": profile, "consent": user.get("consent", False), "verified": user.get("verified", False)})
    safe_reply(update, f"📍 Location set to: *{location}*! 🌍", parse_mode="MarkdownV2")
    return settings(update, context)

def set_bio(update: Update, context: CallbackContext) -> int:
    user_id = update.effective_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    bio = update.message.text.strip()
    if len(bio) > MAX_PROFILE_LENGTH:
        safe_reply(update, f"⚠️ Bio must be under {MAX_PROFILE_LENGTH} characters.", parse_mode="MarkdownV2")
        return BIO
    if not is_safe_message(bio):
        safe_reply(update, "⚠️ Bio contains inappropriate content. Please try again.", parse_mode="MarkdownV2")
        return BIO
    profile["bio"] = bio
    update_user(user_id, {"profile": profile, "consent": user.get("consent", False), "verified": user.get("verified", False)})
    safe_reply(update, f"📝 Bio set to: *{bio}*! ✨", parse_mode="MarkdownV2")
    return settings(update, context)

def cancel(update: Update, context: CallbackContext) -> int:
    safe_reply(update, "❌ Operation cancelled. Use /settings to try again.", parse_mode="MarkdownV2")
    return ConversationHandler.END

def rematch(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        safe_reply(update, "🚫 You are currently banned.", parse_mode="MarkdownV2")
        return
    if not check_rate_limit(user_id):
        safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again.", parse_mode="MarkdownV2")
        return
    if not is_premium(user_id):
        safe_reply(update, "🔄 Re-match is a premium feature. Use /premium to unlock!", parse_mode="MarkdownV2")
        return
    if user_id in user_pairs:
        safe_reply(update, "💬 You're already in a chat. Use /stop to end it first.", parse_mode="MarkdownV2")
        return
    previous_partner = previous_partners.get(user_id)
    if not previous_partner:
        safe_reply(update, "❌ You don't have a previous partner to re-match with.", parse_mode="MarkdownV2")
        return
    if previous_partner in user_pairs:
        safe_reply(update, "❌ Your previous partner is currently in another chat.", parse_mode="MarkdownV2")
        return
    if previous_partner in waiting_users:
        waiting_users.remove(previous_partner)
    user_pairs[user_id] = previous_partner
    user_pairs[previous_partner] = user_id
    safe_reply(update, "🔄 *Re-connected!* You're back with your previous partner! 🗣️", parse_mode="MarkdownV2")
    safe_bot_send_message(context.bot, previous_partner, "🔄 *Re-connected!* Your previous partner is back! 🗣️", parse_mode="MarkdownV2")
    logger.info(f"User {user_id} rematched with {previous_partner}.")
    if is_premium(user_id) or has_premium_feature(user_id, "vaulted_chats"):
        chat_histories[user_id] = chat_histories.get(user_id, [])
    if is_premium(previous_partner) or has_premium_feature(previous_partner, "vaulted_chats"):
        chat_histories[previous_partner] = chat_histories.get(previous_partner, [])

def delete_profile(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        safe_reply(update, "🚫 You are currently banned.", parse_mode="MarkdownV2")
        return
    if not check_rate_limit(user_id):
        safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again.", parse_mode="MarkdownV2")
        return
    delete_user(user_id)
    safe_reply(update, "🗑️ Your profile and data have been deleted. Goodbye! 👋", parse_mode="MarkdownV2")
    logger.info(f"User {user_id} deleted their profile.")

def admin_access(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🚫 Unauthorized.", parse_mode="MarkdownV2")
        logger.info(f"Unauthorized access attempt by user_id={user_id}")
        return
    access_text = (
        "🔐 *Admin Commands* 🔐\n\n"
        "👤 *User Management*\n"
        "• /admin_delete <user_id> - Delete a user’s data\n"
        "• /admin_premium <user_id> <days> - Grant premium\n"
        "• /admin_revoke_premium <user_id> - Revoke premium\n\n"
        "🚫 *Ban Management*\n"
        "• /admin_ban <user_id> <days/permanent> - Ban a user\n"
        "• /admin_unban <user_id> - Unban a user\n\n"
        "📊 *Reports & Info*\n"
        "• /admin_info <user_id> - View user details\n"
        "• /admin_reports - List reported users\n"
        "• /admin_clear_reports <user_id> - Clear reports\n\n"
        "📢 *Broadcast*\n"
        "• /admin_broadcast <message> - Send message to all users\n\n"
        "📋 *User Lists*\n"
        "• /admin_userslist - List all bot users\n"
        "• /premiumuserslist - List premium users\n"
    )
    safe_reply(update, access_text, parse_mode="MarkdownV2")

def admin_delete(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🚫 Unauthorized.", parse_mode="MarkdownV2")
        return
    try:
        target_id = int(context.args[0])
        delete_user(target_id)
        safe_reply(update, f"🗑️ User *{target_id}* data deleted successfully.", parse_mode="MarkdownV2")
        logger.info(f"Admin {user_id} deleted user {target_id}.")
    except (IndexError, ValueError):
        safe_reply(update, "⚠️ Usage: /admin_delete <user_id>", parse_mode="MarkdownV2")

def admin_premium(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🚫 Unauthorized.", parse_mode="MarkdownV2")
        return
    try:
        target_id = int(context.args[0])
        days = int(context.args[1])
        if days <= 0:
            raise ValueError("Days must be positive")
        expiry = int(time.time()) + days * 24 * 3600
        expiry_date = datetime.fromtimestamp(expiry).strftime("%Y-%m-%d")
        user = get_user(target_id)
        update_user(target_id, {
            "premium_expiry": expiry,
            "profile": user.get("profile", {}),
            "consent": user.get("consent", False),
            "verified": user.get("verified", False)
        })
        safe_reply(update, f"🎉 User *{target_id}* granted premium for *{days}* days!", parse_mode="MarkdownV2")
        logger.info(f"Admin {user_id} granted premium to {target_id} for {days} days.")
        notification_text = (
            "🎉 *Congratulations!* You’ve been upgraded to premium! 🌟\n\n"
            f"You now have premium access for *{days} days*, until *{expiry_date}*.\n"
            "Enjoy these benefits:\n"
            "🌟 Priority matching\n"
            "📜 Chat history\n"
            "🔍 Advanced filters\n"
            "✅ Verified badge\n"
            "💬 25 messages/min\n\n"
            "Start exploring with /help or /premium!"
        )
        safe_bot_send_message(context.bot, target_id, notification_text, parse_mode="MarkdownV2")
    except (IndexError, ValueError):
        safe_reply(update, "⚠️ Usage: /admin_premium <user_id> <days>", parse_mode="MarkdownV2")

def admin_revoke_premium(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🚫 Unauthorized.", parse_mode="MarkdownV2")
        return
    try:
        target_id = int(context.args[0])
        user = get_user(target_id)
        update_user(target_id, {
            "premium_expiry": None,
            "profile": user.get("profile", {}),
            "consent": user.get("consent", False),
            "verified": user.get("verified", False),
            "premium_features": {}
        })
        safe_reply(update, f"❌ Premium status revoked for user *{target_id}*.", parse_mode="MarkdownV2")
        logger.info(f"Admin {user_id} revoked premium for {target_id}.")
    except (IndexError, ValueError):
        safe_reply(update, "⚠️ Usage: /admin_revoke_premium <user_id>", parse_mode="MarkdownV2")

def admin_ban(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🚫 Unauthorized.", parse_mode="MarkdownV2")
        return
    try:
        target_id = int(context.args[0])
        ban_type = context.args[1].lower()
        user = get_user(target_id)
        if ban_type == "permanent":
            update_user(target_id, {
                "ban_type": "permanent",
                "ban_expiry": None,
                "profile": user.get("profile", {}),
                "consent": user.get("consent", False),
                "verified": user.get("verified", False)
            })
            safe_reply(update, f"🚫 User *{target_id}* permanently banned.", parse_mode="MarkdownV2")
            logger.info(f"Admin {user_id} permanently banned {target_id}.")
        elif ban_type.isdigit():
            days = int(ban_type)
            expiry = int(time.time()) + days * 24 * 3600
            update_user(target_id, {
                "ban_type": "temporary",
                "ban_expiry": expiry,
                "profile": user.get("profile", {}),
                "consent": user.get("consent", False),
                "verified": user.get("verified", False)
            })
            safe_reply(update, f"🚫 User *{target_id}* banned for *{days}* days.", parse_mode="MarkdownV2")
            logger.info(f"Admin {user_id} banned {target_id} for {days} days.")
        else:
            safe_reply(update, "⚠️ Usage: /admin_ban <user_id> <days/permanent>", parse_mode="MarkdownV2")
            return
        if target_id in user_pairs:
            stop(update, context)
    except (IndexError, ValueError):
        safe_reply(update, "⚠️ Usage: /admin_ban <user_id> <days/permanent>", parse_mode="MarkdownV2")

def admin_unban(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🚫 Unauthorized.", parse_mode="MarkdownV2")
        return
    try:
        target_id = int(context.args[0])
        user = get_user(target_id)
        update_user(target_id, {
            "ban_type": None,
            "ban_expiry": None,
            "profile": user.get("profile", {}),
            "consent": user.get("consent", False),
            "verified": user.get("verified", False)
        })
        safe_reply(update, f"✅ User *{target_id}* unbanned successfully.", parse_mode="MarkdownV2")
        logger.info(f"Admin {user_id} unbanned {target_id}.")
    except (IndexError, ValueError):
        safe_reply(update, "⚠️ Usage: /admin_unban <user_id>", parse_mode="MarkdownV2")

def admin_info(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🚫 Unauthorized.", parse_mode="MarkdownV2")
        return
    try:
        target_id = int(context.args[0])
        user = get_user(target_id)
        if not user:
            safe_reply(update, f"❌ User *{target_id}* not found.", parse_mode="MarkdownV2")
            return
        info = f"👤 *User Info: {target_id}*\n\n"
        info += f"📋 *Profile*: {json.dumps(user.get('profile', {}), indent=2)}\n"
        info += f"✅ *Consent*: {user.get('consent', False)}"
        if user.get("consent_time"):
            info += f" (given at {datetime.fromtimestamp(user['consent_time'])})\n"
        else:
            info += "\n"
        info += f"🌟 *Premium*: {is_premium(target_id)}"
        if user.get("premium_expiry"):
            info += f" (expires at {datetime.fromtimestamp(user['premium_expiry'])})\n"
        else:
            info += "\n"
        info += f"✨ *Features*: {json.dumps(user.get('premium_features', {}), indent=2)}\n"
        info += f"🚫 *Banned*: {is_banned(target_id)}"
        if user.get("ban_type"):
            info += f" ({user['ban_type']}"
            if user["ban_expiry"]:
                info += f", expires at {datetime.fromtimestamp(user['ban_expiry'])}"
            info += ")\n"
        else:
            info += "\n"
        info += f"🔐 *Verified*: {user.get('verified', False)}\n"
        conn = get_db_connection()
        if conn:
            try:
                with conn.cursor() as c:
                    c.execute("SELECT COUNT(*), string_agg(reason, '; ') FROM reports WHERE reported_id = %s", (target_id,))
                    count, reasons = c.fetchone()
                    info += f"🚨 *Reports*: {count}"
                    if reasons:
                        info += f" (Reasons: {reasons})"
            except Exception as e:
                logger.error(f"Failed to fetch reports for {target_id}: {e}")
            finally:
                release_db_connection(conn)
        safe_reply(update, info, parse_mode="MarkdownV2")
        logger.info(f"Admin {user_id} viewed info for {target_id}.")
    except (IndexError, ValueError):
        safe_reply(update, "⚠️ Usage: /admin_info <user_id>", parse_mode="MarkdownV2")

def admin_reports(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🚫 Unauthorized.", parse_mode="MarkdownV2")
        return
    conn = get_db_connection()
    if not conn:
        safe_reply(update, "❌ Error fetching reports due to database issue.", parse_mode="MarkdownV2")
        return
    try:
        with conn.cursor() as c:
            c.execute("""
                SELECT reported_id, COUNT(*), string_agg(reason, '; ')
                FROM reports
                GROUP BY reported_id
                ORDER BY COUNT(*) DESC
                LIMIT 10
            """)
            results = c.fetchall()
            if not results:
                safe_reply(update, "✅ No reported users at the moment.", parse_mode="MarkdownV2")
                return
            report_text = "🚨 *Reported Users (Top 10)* 🚨\n\n"
            for reported_id, count, reasons in results:
                report_text += f"👤 User *{reported_id}*: {count} reports (Reasons: {reasons or 'None'})\n"
            safe_reply(update, report_text, parse_mode="MarkdownV2")
            logger.info(f"Admin {user_id} viewed reported users.")
    except Exception as e:
        logger.error(f"Failed to fetch reports: {e}")
        safe_reply(update, "❌ Error fetching reports.", parse_mode="MarkdownV2")
    finally:
        release_db_connection(conn)

def admin_clear_reports(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🚫 Unauthorized.", parse_mode="MarkdownV2")
        return
    try:
        target_id = int(context.args[0])
        conn = get_db_connection()
        if not conn:
            safe_reply(update, "❌ Error clearing reports due to database issue.", parse_mode="MarkdownV2")
            return
        try:
            with conn.cursor() as c:
                c.execute("DELETE FROM reports WHERE reported_id = %s", (target_id,))
                conn.commit()
                safe_reply(update, f"✅ Reports cleared for user *{target_id}*.", parse_mode="MarkdownV2")
                logger.info(f"Admin {user_id} cleared reports for {target_id}.")
        except Exception as e:
            logger.error(f"Failed to clear reports for {target_id}: {e}")
            safe_reply(update, "❌ Error clearing reports.", parse_mode="MarkdownV2")
        finally:
            release_db_connection(conn)
    except (IndexError, ValueError):
        safe_reply(update, "⚠️ Usage: /admin_clear_reports <user_id>", parse_mode="MarkdownV2")

def admin_broadcast(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🚫 Unauthorized.", parse_mode="MarkdownV2")
        return
    if not context.args:
        safe_reply(update, "⚠️ Usage: /admin_broadcast <message>", parse_mode="MarkdownV2")
        return
    message = " ".join(context.args)
    conn = get_db_connection()
    if not conn:
        safe_reply(update, "❌ Error broadcasting due to database issue.", parse_mode="MarkdownV2")
        return
    try:
        with conn.cursor() as c:
            c.execute("SELECT user_id FROM users")
            users = c.fetchall()
            for (user_id,) in users:
                try:
                    safe_bot_send_message(context.bot, user_id, f"📢 *Announcement*: {message}", parse_mode="MarkdownV2")
                except Exception as e:
                    logger.warning(f"Failed to broadcast to {user_id}: {e}")
            safe_reply(update, "📢 Broadcast sent successfully.", parse_mode="MarkdownV2")
            logger.info(f"Admin {user_id} broadcasted message: {message}")
    except Exception as e:
        logger.error(f"Failed to broadcast: {e}")
        safe_reply(update, "❌ Error sending broadcast.", parse_mode="MarkdownV2")
    finally:
        release_db_connection(conn)

def admin_userslist(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🚫 Unauthorized.", parse_mode="MarkdownV2")
        return
    conn = get_db_connection()
    if not conn:
        safe_reply(update, "❌ Error fetching user list due to database issue.", parse_mode="MarkdownV2")
        return
    try:
        with conn.cursor() as c:
            c.execute("SELECT user_id FROM users ORDER BY user_id LIMIT 50")
            users = c.fetchall()
            if not users:
                safe_reply(update, "❌ No users found.", parse_mode="MarkdownV2")
                return
            user_list = "👥 *All Users (Top 50)* 👥\n\n"
            for (uid,) in users:
                user_list += f"• User ID: *{uid}*\n"
            safe_reply(update, user_list, parse_mode="MarkdownV2")
            logger.info(f"Admin {user_id} viewed users list.")
    except Exception as e:
        logger.error(f"Failed to fetch users list: {e}")
        safe_reply(update, "❌ Error fetching user list.", parse_mode="MarkdownV2")
    finally:
        release_db_connection(conn)

def premiumuserslist(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🚫 Unauthorized.", parse_mode="MarkdownV2")
        return
    conn = get_db_connection()
    if not conn:
        safe_reply(update, "❌ Error fetching premium users due to database issue.", parse_mode="MarkdownV2")
        return
    try:
        with conn.cursor() as c:
            c.execute("SELECT user_id, premium_expiry FROM users WHERE premium_expiry > %s ORDER BY premium_expiry DESC LIMIT 50", (int(time.time()),))
            users = c.fetchall()
            if not users:
                safe_reply(update, "❌ No premium users found.", parse_mode="MarkdownV2")
                return
            user_list = "🌟 *Premium Users (Top 50)* 🌟\n\n"
            for uid, expiry in users:
                expiry_date = datetime.fromtimestamp(expiry).strftime("%Y-%m-%d")
                user_list += f"• User ID: *{uid}* (Expires: *{expiry_date}*)\n"
            safe_reply(update, user_list, parse_mode="MarkdownV2")
            logger.info(f"Admin {user_id} viewed premium users list.")
    except Exception as e:
        logger.error(f"Failed to fetch premium users list: {e}")
        safe_reply(update, "❌ Error fetching premium users.", parse_mode="MarkdownV2")
    finally:
        release_db_connection(conn)

def error_handler(update: Update, context: CallbackContext) -> None:
    logger.error(f"Update {update} caused error {context.error}. Message: {update.message.text if update and update.message else 'N/A'}", exc_info=True)
    if update and update.message:
        safe_reply(update, "❌ An error occurred\\. Please try again or use /help for assistance\\.", parse_mode="MarkdownV2")
    for admin_id in ADMIN_IDS:
        try:
            context.bot.send_message(admin_id, f"⚠️ Bot error: {context.error}")
        except Exception as e:
            logger.warning(f"Failed to notify admin {admin_id}: {e}")

def main() -> None:
    TOKEN = os.getenv("TOKEN")
    if not TOKEN:
        logger.error("No TOKEN found in environment variables.")
        exit(1)
    try:
        updater = Updater(TOKEN, use_context=True)
        dispatcher = updater.dispatcher

        # Conversation handler for profile settings
        conv_handler = ConversationHandler(
            entry_points=[CommandHandler("settings", settings)],
            states={
                GENDER: [
                    CallbackQueryHandler(button, pattern="^(set_gender|set_age|set_tags|set_location|set_bio|set_gender_pref|back_to_settings|back_to_chat)$"),
                    CallbackQueryHandler(button, pattern="^(gender_male|gender_female|gender_other|gender_skip|pref_male|pref_female|pref_any)$")
                ],
                AGE: [MessageHandler(Filters.text & ~Filters.command, set_age)],
                TAGS: [MessageHandler(Filters.text & ~Filters.command, set_tags)],
                LOCATION: [MessageHandler(Filters.text & ~Filters.command, set_location)],
                BIO: [MessageHandler(Filters.text & ~Filters.command, set_bio)],
                CONSENT: [CallbackQueryHandler(consent_handler, pattern="^(consent_agree|consent_disagree)$")],
                VERIFICATION: [MessageHandler(Filters.text & ~Filters.command, verification_handler)]
            },
            fallbacks=[CommandHandler("cancel", cancel)],
            conversation_timeout=300
        )

        # Conversation handler for start flow (consent + verification)
        start_handler = ConversationHandler(
            entry_points=[CommandHandler("start", start)],
            states={
                CONSENT: [CallbackQueryHandler(consent_handler, pattern="^(consent_agree|consent_disagree)$")],
                VERIFICATION: [MessageHandler(Filters.text & ~Filters.command, verification_handler)]
            },
            fallbacks=[CommandHandler("cancel", cancel)],
            conversation_timeout=300
        )

        # Register handlers
        dispatcher.add_handler(start_handler)
        dispatcher.add_handler(conv_handler)
        dispatcher.add_handler(CommandHandler("stop", stop))
        dispatcher.add_handler(CommandHandler("next", next_chat))
        dispatcher.add_handler(CommandHandler("help", help_command))
        dispatcher.add_handler(CommandHandler("premium", premium))
        dispatcher.add_handler(CommandHandler("shine", shine))
        dispatcher.add_handler(CommandHandler("instant", instant))
        dispatcher.add_handler(CommandHandler("vault", vault))
        dispatcher.add_handler(CommandHandler("flare", flare))
        dispatcher.add_handler(CommandHandler("history", history))
        dispatcher.add_handler(CommandHandler("report", report))
        dispatcher.add_handler(CommandHandler("rematch", rematch))
        dispatcher.add_handler(CommandHandler("deleteprofile", delete_profile))
        dispatcher.add_handler(CommandHandler("mood", mood))

        # Admin commands
        dispatcher.add_handler(CommandHandler("admin", admin_access))
        dispatcher.add_handler(CommandHandler("admin_delete", admin_delete))
        dispatcher.add_handler(CommandHandler("admin_premium", admin_premium))
        dispatcher.add_handler(CommandHandler("admin_revoke_premium", admin_revoke_premium))
        dispatcher.add_handler(CommandHandler("admin_ban", admin_ban))
        dispatcher.add_handler(CommandHandler("admin_unban", admin_unban))
        dispatcher.add_handler(CommandHandler("admin_info", admin_info))
        dispatcher.add_handler(CommandHandler("admin_reports", admin_reports))
        dispatcher.add_handler(CommandHandler("admin_clear_reports", admin_clear_reports))
        dispatcher.add_handler(CommandHandler("admin_broadcast", admin_broadcast))
        dispatcher.add_handler(CommandHandler("admin_userslist", admin_userslist))
        dispatcher.add_handler(CommandHandler("premiumuserslist", premiumuserslist))  # Update to premium_users_list if that's the correct function

        # Payment and button handlers
        dispatcher.add_handler(CallbackQueryHandler(buy_premium, pattern="^(buy_flare|buy_instant|buy_shine|buy_mood|buy_vault|buy_premium_pass)$"))
        dispatcher.add_handler(CallbackQueryHandler(set_mood, pattern="^(mood_chill|mood_deep|mood_fun|mood_clear)$"))
        dispatcher.add_handler(CallbackQueryHandler(button))
        dispatcher.add_handler(PreCheckoutQueryHandler(pre_checkout))
        dispatcher.add_handler(MessageHandler(Filters.successful_payment, successful_payment))

        # Message handler
        dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

        # Error handler
        dispatcher.add_error_handler(error_handler)

        # Job queue for cleanup
        updater.job_queue.run_repeating(cleanup_in_memory, interval=3600, first=10)

        # Start bot
        updater.start_polling(allowed_updates=Update.ALL_TYPES)
        logger.info("Bot started successfully.")
        updater.idle()

    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        raise

if __name__ == "__main__":
    main()
           
