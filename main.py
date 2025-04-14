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
            # Create users table if it doesn't exist
            c.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    profile JSONB,
                    consent BOOLEAN DEFAULT FALSE,
                    consent_time BIGINT,
                    premium_expiry BIGINT,
                    ban_type TEXT,
                    ban_expiry BIGINT,
                    verified BOOLEAN DEFAULT FALSE
                )
            """)
            # Check if premium_features column exists
            c.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'users' AND column_name = 'premium_features'
            """)
            if not c.fetchone():
                # Add premium_features column
                c.execute("""
                    ALTER TABLE users
                    ADD COLUMN premium_features JSONB DEFAULT '{}'
                """)
                logger.info("Added premium_features column to users table.")
            # Create reports table
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
# Initialize database
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
            # Try querying with premium_features
            c.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'users' AND column_name = 'premium_features'
            """)
            has_premium_features = bool(c.fetchone())
            if has_premium_features:
                c.execute(
                    "SELECT profile, consent, consent_time, premium_expiry, ban_type, ban_expiry, verified, premium_features "
                    "FROM users WHERE user_id = %s",
                    (user_id,)
                )
            else:
                c.execute(
                    "SELECT profile, consent, consent_time, premium_expiry, ban_type, ban_expiry, verified "
                    "FROM users WHERE user_id = %s",
                    (user_id,)
                )
            result = c.fetchone()
            if result:
                if has_premium_features:
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
                else:
                    return {
                        "profile": result[0] or {},
                        "consent": result[1],
                        "consent_time": result[2],
                        "premium_expiry": result[3],
                        "ban_type": result[4],
                        "ban_expiry": result[5],
                        "verified": result[6],
                        "premium_features": {}  # Fallback
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
            # Check if premium_features column exists
            c.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'users' AND column_name = 'premium_features'
            """)
            has_premium_features = bool(c.fetchone())
            if has_premium_features:
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
            else:
                c.execute("""
                    INSERT INTO users (user_id, profile, consent, consent_time, premium_expiry, ban_type, ban_expiry, verified)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE
                    SET profile = %s, consent = %s, consent_time = %s, premium_expiry = %s, ban_type = %s, ban_expiry = %s, verified = %s
                """, (
                    user_id, json.dumps(data.get("profile", {})), data.get("consent", False), data.get("consent_time"),
                    data.get("premium_expiry"), data.get("ban_type"), data.get("ban_expiry"), data.get("verified", False),
                    json.dumps(data.get("profile", {})), data.get("consent", False), data.get("consent_time"),
                    data.get("premium_expiry"), data.get("ban_type"), data.get("ban_expiry"), data.get("verified", False)
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
    return feature in features and features[feature] > time.time()

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
    """Escape special MarkdownV2 characters."""
    special_chars = r'_*\[\]()~`>#+\-=|{}.!'
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text

def start(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned. Contact support if you believe this is an error.")
        logger.info(f"Banned user {user_id} attempted to start a chat.")
        return ConversationHandler.END
    if not check_rate_limit(user_id):
        update.message.reply_text(f"Please wait {COMMAND_COOLDOWN} seconds before trying again.")
        return ConversationHandler.END
    if user_id in user_pairs:
        update.message.reply_text("You're already in a chat. Use /next to switch or /stop to end.")
        return ConversationHandler.END
    user = get_user(user_id)
    if not user.get("consent"):
        keyboard = [
            [InlineKeyboardButton("I Agree", callback_data="consent_agree")],
            [InlineKeyboardButton("I Disagree", callback_data="consent_disagree")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        update.message.reply_text(
            "Welcome to Talk2Anyone! 🌟\n\n"
            "Connect anonymously with people worldwide. Rules:\n"
            "- No harassment, spam, or inappropriate content.\n"
            "- Respect others at all times.\n"
            "- Report issues with /report.\n"
            "- Violations lead to bans.\n\n"
            "Privacy: We store only your user ID, profile, and consent in a secure database. Use /deleteprofile to erase your data.\n\n"
            "Do you agree to the rules?",
            reply_markup=reply_markup
        )
        return CONSENT
    if not user.get("verified"):
        update.message.reply_text(
            "Please verify your profile to start chatting. Enter a short verification phrase (e.g., 'I’m here to chat respectfully'):"
        )
        return VERIFICATION
    if user_id not in waiting_users:
        if is_premium(user_id) or has_premium_feature(user_id, "shine_profile"):
            waiting_users.insert(0, user_id)
        else:
            waiting_users.append(user_id)
        update.message.reply_text("Looking for a partner... 🔍")
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
        query.message.reply_text("Thank you for agreeing! Please verify your profile next.")
        query.message.reply_text("Enter a short verification phrase (e.g., 'I’m here to chat respectfully'):")
        return VERIFICATION
    else:
        query.message.reply_text("You must agree to the rules to use this bot. Use /start to try again.")
        logger.info(f"User {user_id} declined rules.")
        return ConversationHandler.END

def verification_handler(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
    phrase = update.message.text.strip()
    if len(phrase) < 10 or not is_safe_message(phrase):
        update.message.reply_text("Please provide a valid, respectful verification phrase (min 10 characters).")
        return VERIFICATION
    user = get_user(user_id)
    update_user(user_id, {
        "verified": True,
        "profile": user.get("profile", {}),
        "consent": user.get("consent", False)
    })
    update.message.reply_text("Profile verified! 🎉 Let’s get started.")
    if user_id not in waiting_users:
        if is_premium(user_id) or has_premium_feature(user_id, "shine_profile"):
            waiting_users.insert(0, user_id)
        else:
            waiting_users.append(user_id)
        update.message.reply_text("Looking for a partner... 🔍")
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
                context.bot.send_message(user1, "Connected! Start chatting. 🗣️ Use /help for commands.")
                context.bot.send_message(user2, "Connected! Start chatting. 🗣️ Use /help for commands.")
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
    # Mood Match logic
    if has_premium_feature(user1, "mood_match") and has_premium_feature(user2, "mood_match"):
        mood1 = profile1.get("mood")
        mood2 = profile2.get("mood")
        if mood1 and mood2 and mood1 != mood2:
            return False
    return True

def stop(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        del user_pairs[user_id]
        if partner_id in user_pairs:
            del user_pairs[partner_id]
        context.bot.send_message(partner_id, "Your partner left the chat. Use /start to find a new one.")
        update.message.reply_text("Chat ended. Use /start to begin a new chat.")
        logger.info(f"User {user_id} stopped chat with {partner_id}.")
        if user_id in chat_histories and not has_premium_feature(user_id, "vaulted_chats"):
            del chat_histories[user_id]
    else:
        update.message.reply_text("You're not in a chat. Use /start to find a partner.")

def next_chat(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    if not check_rate_limit(user_id):
        update.message.reply_text(f"Please wait {COMMAND_COOLDOWN} seconds before trying again.")
        return
    stop(update, context)
    if get_user(user_id).get("consent"):
        if user_id not in waiting_users:
            if is_premium(user_id) or has_premium_feature(user_id, "shine_profile"):
                waiting_users.insert(0, user_id)
            else:
                waiting_users.append(user_id)
            update.message.reply_text("Looking for a partner... 🔍")
        match_users(context)
    else:
        update.message.reply_text("Please agree to the rules first by using /start.")

def help_command(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    help_text = (
        "📋 *Talk2Anyone Commands*\n\n"
        "`/start` \\- Start a new anonymous chat\n"
        "`/next` \\- Find a new chat partner\n"
        "`/stop` \\- End the current chat\n"
        "`/help` \\- Show this help message\n"
        "`/report` \\- Report inappropriate behavior\n"
        "`/settings` \\- Customize your profile\n"
        "`/premium` \\- Unlock premium features with Stars\n"
        "`/history` \\- View chat history \\(Premium/Vaulted Chats\\)\n"
        "`/rematch` \\- Reconnect with previous partner \\(Premium\\)\n"
        "`/shine` \\- Boost your profile visibility \\(Premium\\)\n"
        "`/instant` \\- Rematch instantly \\(Premium\\)\n"
        "`/mood` \\- Set a chat mood \\(Premium\\)\n"
        "`/vault` \\- Save chats forever \\(Premium\\)\n"
        "`/flare` \\- Add flair to messages \\(Premium\\)\n"
        "`/deleteprofile` \\- Delete your profile and data\n\n"
        "*Premium Benefits*:\n"
        "\\- Shine Profile: Priority matching\n"
        "\\- Instant Rematch: Reconnect anytime\n"
        "\\- Mood Match: Find similar vibes\n"
        "\\- Vaulted Chats: Save chats forever\n"
        "\\- Flare Messages: Sparkle effects\n"
        "Buy with Stars via `/premium`\\!"
    )
    try:
        update.message.reply_text(help_text, parse_mode="MarkdownV2")
    except Exception as e:
        logger.error(f"Failed to send help message: {e}")
        update.message.reply_text("Error displaying help. Please try again.")

def premium(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    keyboard = [
        [InlineKeyboardButton("Shine Profile (100 Stars)", callback_data="buy_shine")],
        [InlineKeyboardButton("Instant Rematch (50 Stars)", callback_data="buy_instant")],
        [InlineKeyboardButton("Mood Match (150 Stars)", callback_data="buy_mood")],
        [InlineKeyboardButton("Vaulted Chats (200 Stars)", callback_data="buy_vault")],
        [InlineKeyboardButton("Flare Messages (75 Stars)", callback_data="buy_flare")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    premium_text = (
        "🌟 *Premium Features*\n\n"
        "Unlock with Telegram Stars:\n"
        "\\- *Shine Profile*: Be first in line for matches \\(100 Stars\\)\n"
        "\\- *Instant Rematch*: Reconnect anytime \\(50 Stars\\)\n"
        "\\- *Mood Match*: Match by vibe \\(150 Stars\\)\n"
        "\\- *Vaulted Chats*: Save chats forever \\(200 Stars\\)\n"
        "\\- *Flare Messages*: Add sparkle effects \\(75 Stars\\)\n\n"
        "Choose a feature to buy\\!"
    )
    try:
        update.message.reply_text(premium_text, parse_mode="MarkdownV2", reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Failed to send premium message: {e}")
        update.message.reply_text("Error displaying premium features. Please try again.")

def buy_premium(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    if is_banned(user_id):
        query.message.reply_text("You are currently banned.")
        return
    feature_map = {
        "buy_shine": ("Shine Profile", 100, "Boost your profile to the top for 24 hours!", "shine_profile"),
        "buy_instant": ("Instant Rematch", 50, "Reconnect with any past partner instantly!", "instant_rematch"),
        "buy_mood": ("Mood Match", 150, "Match with users sharing your vibe!", "mood_match"),
        "buy_vault": ("Vaulted Chats", 200, "Save your chats forever!", "vaulted_chats"),
        "buy_flare": ("Flare Messages", 75, "Add sparkle effects to your messages!", "flare_messages"),
    }
    choice = query.data
    if choice not in feature_map:
        query.message.reply_text("Invalid feature selected.")
        return
    title, stars, desc, feature_key = feature_map[choice]
    context.bot.send_invoice(
        chat_id=user_id,
        title=title,
        description=desc,
        payload=f"{feature_key}_{user_id}",
        currency="XTR",
        prices=[LabeledPrice(title, stars)],
        start_parameter=f"buy-{feature_key}"
    )
    logger.info(f"Sent invoice for user {user_id}: {title}")

def pre_checkout(update: Update, context: CallbackContext) -> None:
    query = update.pre_checkout_query
    if query.invoice_payload in [f"{key}_{query.from_user.id}" for key in ["shine_profile", "instant_rematch", "mood_match", "vaulted_chats", "flare_messages"]]:
        context.bot.answer_pre_checkout_query(query.id, ok=True)
    else:
        context.bot.answer_pre_checkout_query(query.id, ok=False, error_message="Invalid purchase.")

def successful_payment(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    payment = update.message.successful_payment
    payload = payment.invoice_payload
    current_time = int(time.time())
    feature_map = {
        "shine_profile": (24 * 3600, "Shine Profile activated for 24 hours! 🌟"),
        "instant_rematch": (None, "Instant Rematch unlocked! Use /instant."),  # One-time use
        "mood_match": (30 * 24 * 3600, "Mood Match activated for 30 days! 😊"),
        "vaulted_chats": (None, "Vaulted Chats unlocked forever! 📜"),  # Permanent
        "flare_messages": (7 * 24 * 3600, "Flare Messages activated for 7 days! ✨"),
    }
    for feature, (duration, message) in feature_map.items():
        if payload.startswith(feature):
            user = get_user(user_id)
            features = user.get("premium_features", {})
            expiry = current_time + duration if duration else None
            features[feature] = expiry if expiry else True  # True for permanent
            update_user(user_id, {"premium_features": features})
            update.message.reply_text(message)
            logger.info(f"User {user_id} purchased {feature}")
            break

def shine(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    if not has_premium_feature(user_id, "shine_profile"):
        update.message.reply_text("Shine Profile is a premium feature. Buy with /premium.")
        return
    if user_id not in waiting_users and user_id not in user_pairs:
        waiting_users.insert(0, user_id)
        update.message.reply_text("Your profile is shining! 🔍 First in line for matches.")
        match_users(context)
    else:
        update.message.reply_text("You're already in a chat or waiting.")

def instant(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    if not check_rate_limit(user_id):
        update.message.reply_text(f"Please wait {COMMAND_COOLDOWN} seconds before trying again.")
        return
    if not has_premium_feature(user_id, "instant_rematch"):
        update.message.reply_text("Instant Rematch is a premium feature. Buy with /premium.")
        return
    if user_id in user_pairs:
        update.message.reply_text("You're already in a chat. Use /stop first.")
        return
    previous_partner = previous_partners.get(user_id)
    if not previous_partner:
        update.message.reply_text("No previous partner to rematch with.")
        return
    if previous_partner in user_pairs:
        update.message.reply_text("Your previous partner is in another chat.")
        return
    if previous_partner in waiting_users:
        waiting_users.remove(previous_partner)
    user_pairs[user_id] = previous_partner
    user_pairs[previous_partner] = user_id
    context.bot.send_message(user_id, "Instantly reconnected! 🗣️")
    context.bot.send_message(previous_partner, "Someone reconnected with you! 🗣️")
    logger.info(f"User {user_id} used Instant Rematch with {previous_partner}.")
    # Remove one-time use
    user = get_user(user_id)
    features = user.get("premium_features", {})
    features.pop("instant_rematch", None)
    update_user(user_id, {"premium_features": features})

def mood(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    if not has_premium_feature(user_id, "mood_match"):
        update.message.reply_text("Mood Match is a premium feature. Buy with /premium.")
        return
    keyboard = [
        [InlineKeyboardButton("Chill", callback_data="mood_chill")],
        [InlineKeyboardButton("Deep", callback_data="mood_deep")],
        [InlineKeyboardButton("Fun", callback_data="mood_fun")],
        [InlineKeyboardButton("Clear Mood", callback_data="mood_clear")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    update.message.reply_text("Choose your chat mood:", reply_markup=reply_markup)

def set_mood(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    if not has_premium_feature(user_id, "mood_match"):
        query.message.reply_text("Mood Match is a premium feature. Buy with /premium.")
        return
    choice = query.data
    user = get_user(user_id)
    profile = user.get("profile", {})
    if choice == "mood_clear":
        profile.pop("mood", None)
        query.message.reply_text("Mood cleared.")
    else:
        mood = choice.split("_")[1]
        profile["mood"] = mood
        query.message.reply_text(f"Mood set to: {mood}")
    update_user(user_id, {"profile": profile})

def vault(update: Update, context: Update) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    if not has_premium_feature(user_id, "vaulted_chats"):
        update.message.reply_text("Vaulted Chats is a premium feature. Buy with /premium.")
        return
    if user_id not in chat_histories or not chat_histories[user_id]:
        update.message.reply_text("No chats saved in your vault.")
        return
    history_text = "📜 *Vaulted Chats*\n\n"
    for msg in chat_histories[user_id][-10:]:
        history_text += f"[{msg['time']}]: {msg['text']}\n"
    update.message.reply_text(history_text, parse_mode="Markdown")

def flare(update: Update, context: Update) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    if not has_premium_feature(user_id, "flare_messages"):
        update.message.reply_text("Flare Messages is a premium feature. Buy with /premium.")
        return
    user = get_user(user_id)
    features = user.get("premium_features", {})
    flare_active = features.get("flare_active", False)
    features["flare_active"] = not flare_active
    update_user(user_id, {"premium_features": features})
    update.message.reply_text(f"Flare Messages {'enabled' if not flare_active else 'disabled'}. ✨")

def history(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    if not (is_premium(user_id) or has_premium_feature(user_id, "vaulted_chats")):
        update.message.reply_text("Chat history is a premium feature. Use /premium to unlock.")
        return
    if user_id not in chat_histories or not chat_histories[user_id]:
        update.message.reply_text("No chat history available.")
        return
    history_text = "📜 *Chat History*\n\n"
    for msg in chat_histories[user_id][-10:]:
        history_text += f"[{msg['time']}]: {msg['text']}\n"
    update.message.reply_text(history_text, parse_mode="Markdown")

def report(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    if not check_rate_limit(user_id):
        update.message.reply_text(f"Please wait {COMMAND_COOLDOWN} seconds before trying again.")
        return
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        conn = get_db_connection()
        if not conn:
            update.message.reply_text("Error processing report due to database issue.")
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
                    context.bot.send_message(partner_id, "You’ve been temporarily banned due to multiple reports.")
                    logger.warning(f"User {partner_id} banned temporarily due to {report_count} reports.")
                    stop(update, context)
                update.message.reply_text("Thank you for reporting. We’ll review it. Use /next to find a new partner.")
                logger.info(f"User {user_id} reported user {partner_id}. Reason: {reason}. Total reports: {report_count}.")
        except Exception as e:
            logger.error(f"Failed to log report: {e}")
            update.message.reply_text("Error processing report.")
        finally:
            release_db_connection(conn)
    else:
        update.message.reply_text("You're not in a chat. Use /start to begin.")

def handle_message(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    if not check_message_rate_limit(user_id):
        update.message.reply_text("You're sending messages too fast. Please slow down.")
        logger.info(f"User {user_id} hit message rate limit.")
        return
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        message_text = update.message.text
        if not is_safe_message(message_text):
            update.message.reply_text("Inappropriate content detected. Please keep the chat respectful.")
            logger.info(f"User {user_id} sent unsafe message: {message_text}")
            return
        flare = has_premium_feature(user_id, "flare_messages") and get_user(user_id).get("premium_features", {}).get("flare_active", False)
        final_text = f"✨ {message_text} ✨" if flare else message_text
        context.bot.send_message(partner_id, final_text)
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
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return ConversationHandler.END
    if not check_rate_limit(user_id):
        update.message.reply_text(f"Please wait {COMMAND_COOLDOWN} seconds before trying again.")
        return ConversationHandler.END
    user = get_user(user_id)
    if not user.get("profile"):
        update_user(user_id, {"profile": {}, "consent": user.get("consent", False)})
    keyboard = [
        [InlineKeyboardButton("Gender", callback_data="gender")],
        [InlineKeyboardButton("Age", callback_data="age")],
        [InlineKeyboardButton("Tags", callback_data="tags")],
        [InlineKeyboardButton("Location", callback_data="location")],
        [InlineKeyboardButton("Bio", callback_data="bio")],
        [InlineKeyboardButton("Gender Preference", callback_data="gender_pref")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    update.message.reply_text(
        "Customize your profile and filters (filters may increase matching time):",
        reply_markup=reply_markup
    )
    return GENDER

def button(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    choice = query.data
    if choice == "gender":
        keyboard = [
            [InlineKeyboardButton("Male", callback_data="gender_male")],
            [InlineKeyboardButton("Female", callback_data="gender_female")],
            [InlineKeyboardButton("Other", callback_data="gender_other")],
            [InlineKeyboardButton("Skip", callback_data="gender_skip")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        query.message.reply_text("Select your gender:", reply_markup=reply_markup)
        return GENDER
    elif choice == "age":
        query.message.reply_text("Enter your age (e.g., 25):")
        return AGE
    elif choice == "tags":
        query.message.reply_text(f"Enter tags (e.g., music, gaming) from: {', '.join(ALLOWED_TAGS)}")
        return TAGS
    elif choice == "location":
        query.message.reply_text("Enter your location (e.g., New York):")
        return LOCATION
    elif choice == "bio":
        query.message.reply_text(f"Enter a short bio (max {MAX_PROFILE_LENGTH} characters):")
        return BIO
    elif choice == "gender_pref":
        keyboard = [
            [InlineKeyboardButton("Male", callback_data="pref_male")],
            [InlineKeyboardButton("Female", callback_data="pref_female")],
            [InlineKeyboardButton("Any", callback_data="pref_any")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        query.message.reply_text("Select preferred gender to match with:", reply_markup=reply_markup)
        return GENDER

def set_gender(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    choice = query.data
    user = get_user(user_id)
    profile = user.get("profile", {})
    if choice.startswith("gender_"):
        gender = choice.split("_")[1]
        if gender != "skip":
            profile["gender"] = gender
        else:
            profile.pop("gender", None)
        update_user(user_id, {"profile": profile, "consent": user.get("consent", False), "verified": user.get("verified", False)})
        query.message.reply_text(f"Gender set to: {gender if gender != 'skip' else 'Not specified'}")
    elif choice.startswith("pref_"):
        pref = choice.split("_")[1]
        if pref != "any":
            profile["gender_preference"] = pref
        else:
            profile.pop("gender_preference", None)
        update_user(user_id, {"profile": profile, "consent": user.get("consent", False), "verified": user.get("verified", False)})
        query.message.reply_text(f"Gender preference set to: {pref if pref != 'any' else 'Any'}")
    return settings(update, context)

def set_age(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    try:
        age = int(update.message.text)
        if 13 <= age <= 120:
            profile["age"] = age
            update_user(user_id, {"profile": profile, "consent": user.get("consent", False), "verified": user.get("verified", False)})
            update.message.reply_text(f"Age set to: {age}")
        else:
            update.message.reply_text("Please enter a valid age between 13 and 120.")
            return AGE
    except ValueError:
        update.message.reply_text("Please enter a valid number for age.")
        return AGE
    return settings(update, context)

def set_tags(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    tags = [tag.strip().lower() for tag in update.message.text.split(",") if tag.strip().lower() in ALLOWED_TAGS]
    if not tags and update.message.text.strip():
        update.message.reply_text(f"Invalid tags. Choose from: {', '.join(ALLOWED_TAGS)}")
        return TAGS
    profile["tags"] = tags
    update_user(user_id, {"profile": profile, "consent": user.get("consent", False), "verified": user.get("verified", False)})
    update.message.reply_text(f"Tags set to: {', '.join(tags) if tags else 'None'}")
    return settings(update, context)

def set_location(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    location = update.message.text.strip()
    if len(location) > 100:
        update.message.reply_text("Location must be under 100 characters.")
        return LOCATION
    profile["location"] = location
    update_user(user_id, {"profile": profile, "consent": user.get("consent", False), "verified": user.get("verified", False)})
    update.message.reply_text(f"Location set to: {location}")
    return settings(update, context)

def set_bio(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    bio = update.message.text.strip()
    if len(bio) > MAX_PROFILE_LENGTH:
        update.message.reply_text(f"Bio must be under {MAX_PROFILE_LENGTH} characters.")
        return BIO
    if not is_safe_message(bio):
        update.message.reply_text("Bio contains inappropriate content. Try again.")
        return BIO
    profile["bio"] = bio
    update_user(user_id, {"profile": profile, "consent": user.get("consent", False), "verified": user.get("verified", False)})
    update.message.reply_text(f"Bio set to: {bio}")
    return settings(update, context)

def cancel(update: Update, context: CallbackContext) -> int:
    update.message.reply_text("Operation cancelled.")
    return ConversationHandler.END

def rematch(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    if not check_rate_limit(user_id):
        update.message.reply_text(f"Please wait {COMMAND_COOLDOWN} seconds before trying again.")
        return
    if not is_premium(user_id):
        update.message.reply_text("Re-matching is a premium feature. Use /premium to unlock.")
        return
    if user_id in user_pairs:
        update.message.reply_text("You're already in a chat. Use /stop to end it first.")
        return
    previous_partner = previous_partners.get(user_id)
    if not previous_partner:
        update.message.reply_text("You don't have a previous partner to re-match with.")
        return
    if previous_partner in user_pairs:
        update.message.reply_text("Your previous partner is currently in another chat.")
        return
    if previous_partner in waiting_users:
        waiting_users.remove(previous_partner)
    user_pairs[user_id] = previous_partner
    user_pairs[previous_partner] = user_id
    context.bot.send_message(user_id, "Re-connected with your previous partner! Start chatting. 🗣️")
    context.bot.send_message(previous_partner, "Your previous partner re-connected with you! Start chatting. 🗣️")
    logger.info(f"User {user_id} rematched with {previous_partner}.")
    if is_premium(user_id) or has_premium_feature(user_id, "vaulted_chats"):
        chat_histories[user_id] = chat_histories.get(user_id, [])
    if is_premium(previous_partner) or has_premium_feature(previous_partner, "vaulted_chats"):
        chat_histories[previous_partner] = chat_histories.get(previous_partner, [])

def delete_profile(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    if not check_rate_limit(user_id):
        update.message.reply_text(f"Please wait {COMMAND_COOLDOWN} seconds before trying again.")
        return
    delete_user(user_id)
    update.message.reply_text("Your profile and data have been deleted.")
    logger.info(f"User {user_id} deleted their profile.")

def admin_access(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if user_id not in ADMIN_IDS:
        update.message.reply_text("Unauthorized.")
        logger.info(f"Unauthorized access attempt by user_id={user_id}")
        return
    access_text = (
        "🔐 *Admin Commands*\n\n"
        "`/admin_delete <user_id>` \\- Delete a user’s data\n"
        "`/admin_premium <user_id> <days>` \\- Grant premium\n"
        "`/admin_revoke_premium <user_id>` \\- Revoke premium\n"
        "`/admin_ban <user_id> <days/permanent>` \\- Ban a user\n"
        "`/admin_unban <user_id>` \\- Unban a user\n"
        "`/admin_info <user_id>` \\- View user details\n"
        "`/admin_reports` \\- List reported users\n"
        "`/admin_clear_reports <user_id>` \\- Clear reports\n"
        "`/admin_broadcast <message>` \\- Send message to all users\n"
        "`/admin_userslist` \\- List all bot users with user IDs\n"
        "`/premiumuserslist` \\- List premium users with remaining days\n"
    )
    try:
        update.message.reply_text(access_text, parse_mode="MarkdownV2")
        logger.info(f"Admin {user_id} accessed admin commands list.")
    except Exception as e:
        logger.error(f"Failed to send admin access message: {e}")
        update.message.reply_text("Error displaying admin commands. Please try again.")

def admin_delete(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if user_id not in ADMIN_IDS:
        update.message.reply_text("Unauthorized.")
        return
    try:
        target_id = int(context.args[0])
        delete_user(target_id)
        update.message.reply_text(f"User {target_id} data deleted.")
        logger.info(f"Admin {user_id} deleted user {target_id}.")
    except (IndexError, ValueError):
        update.message.reply_text("Usage: /admin_delete <user_id>")

def admin_premium(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if user_id not in ADMIN_IDS:
        update.message.reply_text("Unauthorized.")
        return
    try:
        target_id = int(context.args[0])
        days = int(context.args[1])
        if days <= 0:
            raise ValueError("Days must be positive")
        expiry = int(time.time()) + days * 24 * 3600
        expiry_date = datetime.fromtimestamp(expiry).strftime("%Y-%m-%d")  # No backslashes
        user = get_user(target_id)
        update_user(target_id, {
            "premium_expiry": expiry,
            "profile": user.get("profile", {}),
            "consent": user.get("consent", False),
            "verified": user.get("verified", False)
        })
        update.message.reply_text(f"User {target_id} granted premium for {days} days.")
        logger.info(f"Admin {user_id} granted premium to {target_id} for {days} days.")
        # Send notification to the target user
        notification_text = (
            "🎉 *Congratulations\\!* You’ve been upgraded to premium\\! 🌟\n\n"
            "You now have premium access for *{} days*\\, until *{}*\\.\n"
            "Enjoy these benefits\\:\n"
            "🌟 Priority matching\n"
            "🌟 Chat history\n"
            "🌟 Advanced filters\n"
            "🌟 Verified badge\n"
            "🌟 25 messages/min\n\n"
            "Start exploring with `/help` or `/premium`\\!"
        ).format(days, escape_markdown_v2(expiry_date))
        logger.debug(f"Sending MarkdownV2 notification to {target_id}: {notification_text}")
        try:
            context.bot.send_message(
                chat_id=target_id,
                text=notification_text,
                parse_mode="MarkdownV2"
            )
            logger.info(f"Sent premium notification to user {target_id}")
        except Exception as e:
            logger.warning(f"Failed to send MarkdownV2 notification to user {target_id}: {e}")
            fallback_text = (
                f"Congratulations! You've been upgraded to premium!\n\n"
                f"You now have premium access for {days} days, until {expiry_date}.\n"
                "Enjoy these benefits:\n"
                "- Priority matching\n"
                "- Chat history\n"
                "- Advanced filters\n"
                "- Verified badge\n"
                "- 25 messages/min\n\n"
                "Start exploring with /help or /premium!"
            )
            try:
                context.bot.send_message(
                    chat_id=target_id,
                    text=fallback_text,
                    parse_mode=None
                )
                logger.info(f"Sent fallback notification to user {target_id}")
            except Exception as e2:
                logger.warning(f"Failed to send fallback notification to user {target_id}: {e2}")
                update.message.reply_text(f"Premium granted, but failed to notify user {target_id}.")
    except (IndexError, ValueError):
        update.message.reply_text("Usage: /admin_premium <user_id> <days>")

def admin_revoke_premium(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if user_id not in ADMIN_IDS:
        update.message.reply_text("Unauthorized.")
        return
    try:
        target_id = int(context.args[0])
        user = get_user(target_id)
        update_user(target_id, {
            "premium_expiry": None,
            "profile": user.get("profile", {}),
            "consent": user.get("consent", False),
            "verified": user.get("verified", False),
            "premium_features": {}  # Clear Stars features too
        })
        update.message.reply_text(f"Premium status revoked for user {target_id}.")
        logger.info(f"Admin {user_id} revoked premium for {target_id}.")
    except (IndexError, ValueError):
        update.message.reply_text("Usage: /admin_revoke_premium <user_id>")

def admin_ban(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if user_id not in ADMIN_IDS:
        update.message.reply_text("Unauthorized.")
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
            update.message.reply_text(f"User {target_id} permanently banned.")
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
            update.message.reply_text(f"User {target_id} banned for {days} days.")
            logger.info(f"Admin {user_id} banned {target_id} for {days} days.")
        else:
            update.message.reply_text("Usage: /admin_ban <user_id> <days/permanent>")
            return
        if target_id in user_pairs:
            stop(update, context)
    except (IndexError, ValueError):
        update.message.reply_text("Usage: /admin_ban <user_id> <days/permanent>")

def admin_unban(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if user_id not in ADMIN_IDS:
        update.message.reply_text("Unauthorized.")
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
        update.message.reply_text(f"User {target_id} unbanned.")
        logger.info(f"Admin {user_id} unbanned {target_id}.")
    except (IndexError, ValueError):
        update.message.reply_text("Usage: /admin_unban <user_id>")

def admin_info(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if user_id not in ADMIN_IDS:
        update.message.reply_text("Unauthorized.")
        return
    try:
        target_id = int(context.args[0])
        user = get_user(target_id)
        if not user:
            update.message.reply_text(f"User {target_id} not found.")
            return
        info = f"User ID: {target_id}\n"
        info += f"Profile: {user.get('profile', {})}\n"
        info += f"Consent: {user.get('consent', False)}"
        if user.get("consent_time"):
            info += f" (-given at {datetime.fromtimestamp(user['consent_time'])})\n"
        else:
            info += "\n"
        info += f"Premium: {is_premium(target_id)}"
        if user.get("premium_expiry"):
            info += f" (expires at {datetime.fromtimestamp(user['premium_expiry'])})\n"
        else:
            info += "\n"
        info += f"Features: {user.get('premium_features', {})}\n"
        info += f"Banned: {is_banned(target_id)}"
        if user.get("ban_type"):
            info += f" ({user['ban_type']}"
            if user["ban_expiry"]:
                info += f", expires at {datetime.fromtimestamp(user['ban_expiry'])}"
            info += ")\n"
        else:
            info += "\n"
        info += f"Verified: {user.get('verified', False)}\n"
        conn = get_db_connection()
        if conn:
            try:
                with conn.cursor() as c:
                    c.execute("SELECT COUNT(*), string_agg(reason, '; ') FROM reports WHERE reported_id = %s", (target_id,))
                    count, reasons = c.fetchone()
                    info += f"Reports: {count}"
                    if reasons:
                        info += f" (Reasons: {reasons})"
            except Exception as e:
                logger.error(f"Failed to fetch reports for {target_id}: {e}")
            finally:
                release_db_connection(conn)
        update.message.reply_text(info)
        logger.info(f"Admin {user_id} viewed info for {target_id}.")
    except (IndexError, ValueError):
        update.message.reply_text("Usage: /admin_info <user_id>")

def admin_reports(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if user_id not in ADMIN_IDS:
        update.message.reply_text("Unauthorized.")
        return
    conn = get_db_connection()
    if not conn:
        update.message.reply_text("Error fetching reports due to database issue.")
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
                update.message.reply_text("No reported users.")
                return
            report_text = "Reported Users (Top 10):\n"
            for reported_id, count, reasons in results:
                report_text += f"User {reported_id}: {count} reports (Reasons: {reasons or 'None'})\n"
            update.message.reply_text(report_text)
            logger.info(f"Admin {user_id} viewed reported users.")
    except Exception as e:
        logger.error(f"Failed to fetch reports: {e}")
        update.message.reply_text("Error fetching reports.")
    finally:
        release_db_connection(conn)

def admin_clear_reports(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if user_id not in ADMIN_IDS:
        update.message.reply_text("Unauthorized.")
        return
    try:
        target_id = int(context.args[0])
        conn = get_db_connection()
        if not conn:
            update.message.reply_text("Error clearing reports due to database issue.")
            return
        try:
            with conn.cursor() as c:
                c.execute("DELETE FROM reports WHERE reported_id = %s", (target_id,))
                conn.commit()
                update.message.reply_text(f"Reports cleared for user {target_id}.")
                logger.info(f"Admin {user_id} cleared reports for {target_id}.")
        except Exception as e:
            logger.error(f"Failed to clear reports: {e}")
            update.message.reply_text("Error clearing reports.")
        finally:
            release_db_connection(conn)
    except (IndexError, ValueError):
        update.message.reply_text("Usage: /admin_clear_reports <user_id>")

def admin_broadcast(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if user_id not in ADMIN_IDS:
        update.message.reply_text("Unauthorized.")
        return
    if not context.args:
        update.message.reply_text("Usage: /admin_broadcast <message>")
        return
    message = " ".join(context.args)
    if not is_safe_message(message):
        update.message.reply_text("Broadcast message contains inappropriate content.")
        return
    conn = get_db_connection()
    if not conn:
        update.message.reply_text("Error sending broadcast due to database issue.")
        return
    try:
        with conn.cursor() as c:
            c.execute("SELECT user_id FROM users")
            user_ids = [row[0] for row in c.fetchall()]
            for uid in user_ids:
                try:
                    context.bot.send_message(uid, f"📢 Broadcast: {message}")
                except Exception as e:
                    logger.warning(f"Failed to send broadcast to {uid}: {e}")
            update.message.reply_text(f"Broadcast sent to {len(user_ids)} users.")
            logger.info(f"Admin {user_id} sent broadcast: {message}")
    except Exception as e:
        logger.error(f"Failed to send broadcast: {e}")
        update.message.reply_text("Error sending broadcast.")
    finally:
        release_db_connection(conn)

def admin_userslist(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if user_id not in ADMIN_IDS:
        update.message.reply_text("Unauthorized.")
        logger.info(f"Unauthorized access attempt by user_id={user_id}")
        return
    conn = get_db_connection()
    if not conn:
        update.message.reply_text("Error fetching users list due to database issue.")
        logger.error("Failed to get database connection for /admin_userslist")
        return
    try:
        with conn.cursor() as c:
            c.execute("SELECT user_id FROM users ORDER BY user_id")
            users = c.fetchall()
            if not users:
                update.message.reply_text("No users found.")
                logger.info(f"Admin {user_id} requested users list: no users found")
                return
            user_list = "*Bot Users List*\n\n"
            for user in users:
                user_list += f"User ID: `{user[0]}`\n"
            user_list += f"\n*Total Users*: `{len(users)}`"
            update.message.reply_text(user_list, parse_mode="MarkdownV2")
            logger.info(f"Admin {user_id} viewed users list: {len(users)} users")
    except Exception as e:
        logger.error(f"Failed to fetch users list: {e}")
        update.message.reply_text("Error fetching users list.")
    finally:
        release_db_connection(conn)

def premium_users_list(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if user_id not in ADMIN_IDS:
        update.message.reply_text("Unauthorized.")
        logger.info(f"Unauthorized access attempt by user_id={user_id}")
        return
    conn = get_db_connection()
    if not conn:
        update.message.reply_text("Error fetching premium users list due to database issue.")
        logger.error("Failed to get database connection for /premiumuserslist")
        return
    try:
        current_time = int(time.time())
        with conn.cursor() as c:
            c.execute(
                "SELECT user_id, premium_expiry, premium_features FROM users WHERE premium_expiry > %s OR premium_features != '{}'",
                (current_time,)
            )
            users = c.fetchall()
            if not users:
                update.message.reply_text("No premium users found.")
                logger.info(f"Admin {user_id} requested premium users list: no users found")
                return
            user_list = "*Premium Users List*\n\n"
            for user_id, premium_expiry, features in users:
                if premium_expiry and premium_expiry > current_time:
                    remaining_days = (premium_expiry - current_time + 24 * 3600 - 1) // (24 * 3600)
                    user_list += f"User ID: `{user_id}` \\- {remaining_days} days remaining\n"
                if features:
                    active = [k for k, v in features.items() if v is True or (isinstance(v, int) and v > current_time)]
                    if active:
                        user_list += f"User ID: `{user_id}` \\- Features: {', '.join(active)}\n"
            user_list += f"\n*Total Premium Users*: `{len(users)}`"
            update.message.reply_text(user_list, parse_mode="MarkdownV2")
            logger.info(f"Admin {user_id} viewed premium users list: {len(users)} users")
    except Exception as e:
        logger.error(f"Failed to fetch premium users list: {e}")
        update.message.reply_text("Error fetching premium users list.")
    finally:
        release_db_connection(conn)

def error_handler(update: Update, context: CallbackContext) -> None:
    logger.error(f"Update {update} caused error {context.error}", exc_info=True)
    if update and update.message:
        update.message.reply_text("An error occurred. Please try again or use /help for assistance.")
    for admin_id in ADMIN_IDS:
        try:
            context.bot.send_message(admin_id, f"Bot error: {context.error}")
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

        start_handler = ConversationHandler(
            entry_points=[CommandHandler("start", start)],
            states={
                CONSENT: [CallbackQueryHandler(consent_handler, pattern="^consent_")],
                VERIFICATION: [MessageHandler(Filters.text & ~Filters.command, verification_handler)],
            },
            fallbacks=[CommandHandler("cancel", cancel)],
        )
        settings_handler = ConversationHandler(
            entry_points=[CommandHandler("settings", settings)],
            states={
                GENDER: [CallbackQueryHandler(button), CallbackQueryHandler(set_gender, pattern="^(gender_|pref_)")],
                AGE: [MessageHandler(Filters.text & ~Filters.command, set_age)],
                TAGS: [MessageHandler(Filters.text & ~Filters.command, set_tags)],
                LOCATION: [MessageHandler(Filters.text & ~Filters.command, set_location)],
                BIO: [MessageHandler(Filters.text & ~Filters.command, set_bio)],
            },
            fallbacks=[CommandHandler("cancel", cancel)],
        )
        mood_handler = ConversationHandler(
            entry_points=[CommandHandler("mood", mood)],
            states={
                GENDER: [CallbackQueryHandler(set_mood, pattern="^mood_")],
            },
            fallbacks=[CommandHandler("cancel", cancel)],
        )

        dispatcher.add_handler(start_handler)
        dispatcher.add_handler(settings_handler)
        dispatcher.add_handler(mood_handler)
        dispatcher.add_handler(CommandHandler("stop", stop))
        dispatcher.add_handler(CommandHandler("next", next_chat))
        dispatcher.add_handler(CommandHandler("help", help_command))
        dispatcher.add_handler(CommandHandler("premium", premium))
        dispatcher.add_handler(CallbackQueryHandler(buy_premium, pattern="^buy_"))
        dispatcher.add_handler(PreCheckoutQueryHandler(pre_checkout))
        dispatcher.add_handler(MessageHandler(Filters.successful_payment, successful_payment))
        dispatcher.add_handler(CommandHandler("history", history))
        dispatcher.add_handler(CommandHandler("report", report))
        dispatcher.add_handler(CommandHandler("rematch", rematch))
        dispatcher.add_handler(CommandHandler("shine", shine))
        dispatcher.add_handler(CommandHandler("instant", instant))
        dispatcher.add_handler(CommandHandler("vault", vault))
        dispatcher.add_handler(CommandHandler("flare", flare))
        dispatcher.add_handler(CommandHandler("deleteprofile", delete_profile))
        dispatcher.add_handler(CommandHandler("adminaccess", admin_access))
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
        dispatcher.add_handler(CommandHandler("premiumuserslist", premium_users_list))
        dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))
        dispatcher.add_error_handler(error_handler)

        updater.job_queue.run_repeating(cleanup_in_memory, interval=3600, first=60)
        logger.info("Starting Talk2Anyone bot...")
        updater.start_polling(drop_pending_updates=True)
        updater.idle()
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        exit(1)

if __name__ == "__main__":
    main()
