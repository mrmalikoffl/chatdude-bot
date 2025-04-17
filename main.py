import os
import time
import random
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Tuple, Dict, Any
from threading import Lock
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, InputMediaPhoto
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    CallbackQueryHandler,
    ConversationHandler,
    CallbackContext,
    PreCheckoutQueryHandler,
)
from telegram.utils.helpers import escape_markdown_v2
from telegram.error import TelegramError

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Constants
CONSENT, NAME, AGE, GENDER, LOCATION, VERIFICATION, TAGS, SETTINGS = range(8)
COMMAND_COOLDOWN = 30  # Seconds for rate limiting
REPORT_THRESHOLD = 3
TEMP_BAN_DURATION = 24 * 3600  # 24 hours in seconds
ALLOWED_TAGS = [
    "music", "movies", "gaming", "sports", "tech",
    "food", "travel", "books", "art", "fashion"
]
ADMIN_IDS = [5975525252]  # Replace with actual admin Telegram IDs

# Thread-safe data structures
user_pairs = {}
waiting_users = []
user_cache = {}
chat_histories = {}
user_activities = {}
last_command_times = {}
user_pairs_lock = Lock()
waiting_users_lock = Lock()
chat_histories_lock = Lock()

# MongoDB setup
PYMONGO_AVAILABLE = True
try:
    import pymongo
except ImportError:
    PYMONGO_AVAILABLE = False
    logger.warning("PyMongo not installed. Falling back to in-memory cache.")

db = None

def init_mongodb():
    """Initialize MongoDB connection with error handling."""
    uri = os.getenv("MONGODB_URI")
    if not uri:
        logger.error("MONGODB_URI environment variable not set")
        raise ValueError("MONGODB_URI not set")
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")  # Test connection
        logger.info("Successfully connected to MongoDB")
        return client.get_database("talk2anyone")
    except pymongo.errors.ServerSelectionTimeoutError as e:
        logger.error(f"Failed to connect to MongoDB: Server selection timeout - {e}")
        raise
    except pymongo.errors.ConnectionError as e:
        logger.error(f"Failed to connect to MongoDB: Connection error - {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error connecting to MongoDB: {e}")
        raise

def get_db_collection(collection_name: str):
    """Get MongoDB collection with validation."""
    if not PYMONGO_AVAILABLE or db is None:
        logger.error(f"Cannot access collection {collection_name}: MongoDB not available")
        raise RuntimeError("MongoDB not available")
    try:
        return db[collection_name]
    except Exception as e:
        logger.error(f"Failed to access collection {collection_name}: {e}")
        raise

def get_user(user_id: int) -> dict:
    """Retrieve user data from MongoDB or cache."""
    if PYMONGO_AVAILABLE and db is not None:
        try:
            users = get_db_collection("users")
            user = users.find_one({"user_id": user_id})
            return user or {"user_id": user_id, "profile": {}, "premium_features": {}, "consent": False, "verified": False}
        except pymongo.errors.PyMongoError as e:
            logger.error(f"Failed to fetch user {user_id} from MongoDB: {e}")
            return user_cache.get(user_id, {"user_id": user_id, "profile": {}, "premium_features": {}, "consent": False, "verified": False})
    logger.debug(f"Using cache for user {user_id} (MongoDB unavailable)")
    return user_cache.get(user_id, {"user_id": user_id, "profile": {}, "premium_features": {}, "consent": False, "verified": False})

def update_user(user_id: int, data: dict) -> bool:
    """Update user data in MongoDB and cache."""
    if PYMONGO_AVAILABLE and db is not None:
        try:
            users = get_db_collection("users")
            result = users.update_one({"user_id": user_id}, {"$set": data}, upsert=True)
            if result.matched_count > 0 or result.upserted_id is not None:
                user_cache[user_id] = {**user_cache.get(user_id, {}), **data}  # Sync cache
                return True
            logger.warning(f"No user updated or upserted for user_id {user_id}")
            return False
        except pymongo.errors.PyMongoError as e:
            logger.error(f"Failed to update user {user_id} in MongoDB: {e}")
            return False
    logger.debug(f"Updating cache for user {user_id}")
    user_cache[user_id] = {**user_cache.get(user_id, {}), **data}
    return True

def delete_user(user_id: int) -> bool:
    """Delete user data from MongoDB and cache."""
    if PYMONGO_AVAILABLE and db is not None:
        try:
            users = get_db_collection("users")
            result = users.delete_one({"user_id": user_id})
            if result.deleted_count > 0:
                if user_id in user_cache:
                    del user_cache[user_id]  # Sync cache
                return True
            logger.debug(f"No user found to delete for user_id {user_id}")
            return False
        except pymongo.errors.PyMongoError as e:
            logger.error(f"Failed to delete user {user_id} from MongoDB: {e}")
            return False
    logger.debug(f"Deleting user {user_id} from cache")
    if user_id in user_cache:
        del user_cache[user_id]
        return True
    logger.debug(f"No user found in cache for user_id {user_id}")
    return False

def is_banned(user_id: int) -> bool:
    user = get_user(user_id)
    if not user:
        return False
    ban_type = user.get("ban_type")
    if ban_type == "permanent":
        return True
    if ban_type == "temporary":
        ban_expiry = user.get("ban_expiry")
        return ban_expiry and ban_expiry > int(time.time())
    return False

def has_premium_feature(user_id: int, feature: str) -> bool:
    user = get_user(user_id)
    features = user.get("premium_features", {})
    current_time = int(time.time())
    if feature == "instant_rematch":
        return features.get("instant_rematch_count", 0) > 0
    value = features.get(feature)
    return value is True or (isinstance(value, int) and value > current_time)

def check_rate_limit(user_id: int) -> bool:
    last_time = last_command_times.get(user_id, 0)
    current_time = time.time()
    if current_time - last_time < COMMAND_COOLDOWN:
        return False
    last_command_times[user_id] = current_time
    return True

def safe_reply(update: Update, text: str, reply_markup=None, parse_mode="Markdown"):
    try:
        if update.callback_query:
            update.callback_query.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        else:
            update.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except TelegramError as e:
        logger.error(f"Failed to send reply: {e}")

def safe_bot_send_message(bot, chat_id: int, text: str, parse_mode="Markdown"):
    try:
        bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode)
    except TelegramError as e:
        logger.error(f"Failed to send message to {chat_id}: {e}")

def send_channel_notification(bot, message: str):
    channel_id = os.getenv("CHANNEL_ID")
    if channel_id:
        try:
            bot.send_message(chat_id=channel_id, text=message, parse_mode="Markdown")
        except TelegramError as e:
            logger.error(f"Failed to send notification to channel {channel_id}: {e}")

def start(update: Update, context: CallbackContext) -> int:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return ConversationHandler.END
    user = get_user(user_id)
    if user.get("consent", False):
        profile = user.get("profile", {})
        if all(key in profile for key in ["name", "age", "gender", "location"]):
            safe_reply(update, "🎉 Welcome back! Use /next to start chatting or /settings to update your profile.")
            return ConversationHandler.END
    keyboard = [
        [InlineKeyboardButton("✅ Accept", callback_data="consent_accept")],
        [InlineKeyboardButton("❌ Decline", callback_data="consent_decline")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    safe_reply(update, "🤝 Please accept our Terms & Conditions and Privacy Policy to continue:\n\n[Read here](https://example.com)", reply_markup=reply_markup)
    update_user(user_id, {"setup_state": "CONSENT"})
    context.user_data["state"] = CONSENT
    return CONSENT

def consent_handler(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    if query.data == "consent_accept":
        update_user(user_id, {"consent": True, "setup_state": "NAME"})
        context.user_data["state"] = NAME
        safe_reply(update, "🧑 Please enter your name:")
        return NAME
    else:
        safe_reply(update, "❌ You must accept the Terms & Conditions to use this bot.")
        return ConversationHandler.END

def set_name(update: Update, context: CallbackContext) -> int:
    user_id = update.effective_user.id
    name = update.message.text.strip()
    if len(name) > 50:
        safe_reply(update, "⚠️ Name too long. Please use 50 characters or fewer.")
        return NAME
    user = get_user(user_id)
    profile = user.get("profile", {})
    profile["name"] = name
    update_user(user_id, {"profile": profile, "setup_state": "AGE"})
    context.user_data["state"] = AGE
    safe_reply(update, "🎂 Please enter your age:")
    return AGE

def set_age(update: Update, context: CallbackContext) -> int:
    user_id = update.effective_user.id
    try:
        age = int(update.message.text.strip())
        if not 18 <= age <= 120:
            safe_reply(update, "⚠️ Please enter a valid age between 18 and 120.")
            return AGE
        user = get_user(user_id)
        profile = user.get("profile", {})
        profile["age"] = age
        update_user(user_id, {"profile": profile, "setup_state": "GENDER"})
        context.user_data["state"] = GENDER
        keyboard = [
            [InlineKeyboardButton("👨 Male", callback_data="gender_male"),
             InlineKeyboardButton("👩 Female", callback_data="gender_female")],
            [InlineKeyboardButton("🌈 Other", callback_data="gender_other")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        safe_reply(update, "👤 Please select your gender:", reply_markup=reply_markup)
        return GENDER
    except ValueError:
        safe_reply(update, "⚠️ Please enter a valid number for your age.")
        return AGE

def set_gender(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    gender = query.data.split("_")[1].capitalize()
    user = get_user(user_id)
    profile = user.get("profile", {})
    profile["gender"] = gender
    update_user(user_id, {"profile": profile, "setup_state": "LOCATION"})
    context.user_data["state"] = LOCATION
    safe_reply(update, "📍 Please enter your location (city or country):")
    return LOCATION

def set_location(update: Update, context: CallbackContext) -> int:
    user_id = update.effective_user.id
    location = update.message.text.strip()
    if len(location) > 100:
        safe_reply(update, "⚠️ Location too long. Please use 100 characters or fewer.")
        return LOCATION
    user = get_user(user_id)
    profile = user.get("profile", {})
    profile["location"] = location
    update_user(user_id, {
        "profile": profile,
        "setup_state": None,
        "created_at": user.get("created_at", int(time.time())),
        "consent": user.get("consent", False),
        "verified": user.get("verified", False)
    })
    safe_reply(update, "🎉 Profile setup complete! Use /next to start chatting or /settings to customize further.")
    return ConversationHandler.END

def verify_emoji(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    if query.data == "emoji_correct":
        update_user(user_id, {"verified": True, "setup_state": None})
        safe_reply(update, "✅ Verification successful! Use /next to start chatting.")
        return ConversationHandler.END
    else:
        safe_reply(update, "❌ Incorrect emoji. Please try again.")
        return VERIFICATION

def help_command(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    keyboard = [
        [InlineKeyboardButton("🚀 Start Chat", callback_data="start_chat"),
         InlineKeyboardButton("🔍 Next Partner", callback_data="next_chat")],
        [InlineKeyboardButton("🛑 Stop Chat", callback_data="stop_chat"),
         InlineKeyboardButton("⚙️ Settings", callback_data="settings_menu")],
        [InlineKeyboardButton("💎 Premium", callback_data="premium_menu"),
         InlineKeyboardButton("📜 History", callback_data="history_menu")],
        [InlineKeyboardButton("🚨 Report", callback_data="report_user"),
         InlineKeyboardButton("🔄 Rematch", callback_data="rematch_partner")],
        [InlineKeyboardButton("🗑️ Delete Profile", callback_data="delete_profile")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    help_text = (
        "🤖 *Talk2Anyone Bot Commands* 🤖\n\n"
        "🌟 *Basic Commands*:\n"
        "- /start: Begin or restart the bot 🚀\n"
        "- /next: Find a new chat partner 🔍\n"
        "- /stop: End current chat 🛑\n"
        "- /settings: Customize your profile ⚙️\n\n"
        "💎 *Premium Features*:\n"
        "- /premium: View and buy premium features 💰\n"
        "- /shine: Boost your profile visibility 🌟\n"
        "- /instant: Reconnect with past partners 🔄\n"
        "- /flare: Add sparkle to your messages ✨\n"
        "- /mood: Set your chat mood 😊\n"
        "- /vault: Save your chats 📜\n"
        "- /history: View saved chat history 📖\n\n"
        "🛡️ *Safety & Support*:\n"
        "- /report: Report inappropriate behavior 🚨\n"
        "- /deleteprofile: Delete your data 🗑️\n"
        "- /help: Show this menu 📋"
    )
    safe_reply(update, help_text, reply_markup=reply_markup)

def next_chat(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        del user_pairs[user_id]
        del user_pairs[partner_id]
        safe_reply(update, "👋 Chat ended. Looking for a new partner...")
        safe_bot_send_message(context.bot, partner_id, "👋 Your partner has left the chat. Use /next to find a new one.")
    if user_id not in waiting_users:
        if has_premium_feature(user_id, "shine_profile"):
            waiting_users.insert(0, user_id)
        else:
            waiting_users.append(user_id)
        safe_reply(update, "🔍 Looking for a chat partner...")
    match_users(context)

def stop(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        del user_pairs[user_id]
        del user_pairs[partner_id]
        safe_reply(update, "👋 Chat ended. Use /next to find a new partner.")
        safe_bot_send_message(context.bot, partner_id, "👋 Your partner has left the chat. Use /next to find a new one.")
    elif user_id in waiting_users:
        waiting_users.remove(user_id)
        safe_reply(update, "🛑 Stopped waiting. Use /next to start again.")
    else:
        safe_reply(update, "❓ You're not in a chat or waiting. Use /next to start.")

def match_users(context: CallbackContext) -> None:
    if len(waiting_users) < 2:
        return
    user1 = waiting_users.pop(0)
    user2 = waiting_users.pop(0)
    user_pairs[user1] = user2
    user_pairs[user2] = user1
    user1_data = get_user(user1)
    user2_data = get_user(user2)
    user1_profile = user1_data.get("profile", {})
    user2_profile = user2_data.get("profile", {})
    user1_message = (
        "🎉 *Connected!* 🎉\n\n"
        f"🧑 *Name*: {user2_profile.get('name', 'Anonymous')}\n"
        f"🎂 *Age*: {user2_profile.get('age', 'Not set')}\n"
        f"👤 *Gender*: {user2_profile.get('gender', 'Not set')}\n"
        f"📍 *Location*: {user2_profile.get('location', 'Not set')}\n\n"
        "Start chatting! 🗣️ Use /stop to end or /next to switch."
    )
    user2_message = (
        "🎉 *Connected!* 🎉\n\n"
        f"🧑 *Name*: {user1_profile.get('name', 'Anonymous')}\n"
        f"🎂 *Age*: {user1_profile.get('age', 'Not set')}\n"
        f"👤 *Gender*: {user1_profile.get('gender', 'Not set')}\n"
        f"📍 *Location*: {user1_profile.get('location', 'Not set')}\n\n"
        "Start chatting! 🗣️ Use /stop to end or /next to switch."
    )
    safe_bot_send_message(context.bot, user1, user1_message)
    safe_bot_send_message(context.bot, user2, user2_message)
    for user_id in [user1, user2]:
        if has_premium_feature(user_id, "vaulted_chats"):
            chat_histories[user_id] = chat_histories.get(user_id, [])
    user1_data["profile"]["past_partners"] = user1_data.get("profile", {}).get("past_partners", []) + [user2]
    user2_data["profile"]["past_partners"] = user2_data.get("profile", {}).get("past_partners", []) + [user1]
    update_user(user1, {"profile": user1_data["profile"]})
    update_user(user2, {"profile": user2_data["profile"]})

def premium(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return
    keyboard = [
        [InlineKeyboardButton("✨ Flare Messages (100 Stars)", callback_data="buy_flare_messages"),
         InlineKeyboardButton("🔄 Instant Rematch (100 Stars)", callback_data="buy_instant_rematch")],
        [InlineKeyboardButton("🌟 Shine Profile (250 Stars)", callback_data="buy_shine_profile"),
         InlineKeyboardButton("😊 Mood Match (250 Stars)", callback_data="buy_mood_match")],
        [InlineKeyboardButton("👤 Partner Details (500 Stars)", callback_data="buy_partner_details"),
         InlineKeyboardButton("📜 Vaulted Chats (500 Stars)", callback_data="buy_vaulted_chats")],
        [InlineKeyboardButton("🎉 Premium Pass (1000 Stars)", callback_data="buy_premium_pass")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    premium_text = (
        "💎 *Premium Features* 💎\n\n"
        "Unlock exclusive features with Telegram Stars:\n"
        "- *Flare Messages*: Add sparkle to your messages ✨ (7 days)\n"
        "- *Instant Rematch*: Reconnect with past partners 🔄 (1 use)\n"
        "- *Shine Profile*: Get priority in matching 🌟 (24 hours)\n"
        "- *Mood Match*: Set your chat mood 😊 (30 days)\n"
        "- *Partner Details*: See detailed partner profiles 👤 (30 days)\n"
        "- *Vaulted Chats*: Save chat history forever 📜\n"
        "- *Premium Pass*: All features + 5 Instant Rematches 🎉 (30 days)\n\n"
        "Select an option below:"
    )
    safe_reply(update, premium_text, reply_markup=reply_markup)

def buy_premium(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    feature = query.data.split("_")[1:]
    if not feature:
        safe_reply(update, "❌ Invalid selection. Please try again.")
        return
    feature = "_".join(feature)
    feature_map = {
        "flare_messages": ("Flare Messages", 100, "Add sparkle to your messages! ✨"),
        "instant_rematch": ("Instant Rematch", 100, "Reconnect with a past partner! 🔄"),
        "shine_profile": ("Shine Profile", 250, "Get priority in matching! 🌟"),
        "mood_match": ("Mood Match", 250, "Set your chat mood! 😊"),
        "partner_details": ("Partner Details", 500, "See detailed partner profiles! 👤"),
        "vaulted_chats": ("Vaulted Chats", 500, "Save your chat history forever! 📜"),
        "premium_pass": ("Premium Pass", 1000, "Unlock all features + 5 Instant Rematches! 🎉")
    }
    if feature not in feature_map:
        safe_reply(update, "❌ Invalid feature. Please try again.")
        return
    title, stars, description = feature_map[feature]
    payload = f"{feature}_{user_id}"
    context.bot.send_invoice(
        chat_id=user_id,
        title=title,
        description=description,
        payload=payload,
        provider_token="",  # Set to empty for Telegram Stars
        currency="XTR",
        prices=[{"label": title, "amount": stars}],
        start_parameter=f"buy-{feature}"
    )

def pre_checkout(update: Update, context: CallbackContext) -> None:
    query = update.pre_checkout_query
    if query.currency != "XTR":
        context.bot.answer_pre_checkout_query(
            query.id, ok=False, error_message="Only Telegram Stars payments are supported."
        )
        return
    valid_payloads = [f"{key}_{query.from_user.id}" for key in ["flare_messages", "instant_rematch", "shine_profile", "mood_match", "partner_details", "vaulted_chats", "premium_pass"]]
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
        "flare_messages": (7 * 24 * 3600, "✨ *Flare Messages* activated for 7 days!", "Flare Messages", 100),
        "instant_rematch": (None, "🔄 *Instant Rematch* unlocked! Use /instant to reconnect.", "Instant Rematch", 100),
        "shine_profile": (24 * 3600, "🌟 *Shine Profile* activated for 24 hours!", "Shine Profile", 250),
        "mood_match": (30 * 24 * 3600, "😊 *Mood Match* activated for 30 days!", "Mood Match", 250),
        "partner_details": (30 * 24 * 3600, "👤 *Partner Details* unlocked for 30 days!", "Partner Details", 500),
        "vaulted_chats": (None, "📜 *Vaulted Chats* unlocked forever!", "Vaulted Chats", 500),
        "premium_pass": (30 * 24 * 3600, "🎉 *Premium Pass* activated! Enjoy all features for 30 days + 5 Instant Rematches!", "Premium Pass", 1000),
    }
    user = get_user(user_id)
    features = user.get("premium_features", {})
    premium_expiry = user.get("premium_expiry")
    profile = user.get("profile", {})
    for feature, (duration, message, feature_name, stars) in feature_map.items():
        if payload.startswith(feature):
            if feature == "premium_pass":
                new_expiry = max(premium_expiry or current_time, current_time + 30 * 24 * 3600)
                features.update({
                    "shine_profile": current_time + 30 * 24 * 3600,
                    "mood_match": current_time + 30 * 24 * 3600,
                    "partner_details": current_time + 30 * 24 * 3600,
                    "vaulted_chats": True,
                    "flare_messages": current_time + 30 * 24 * 3600,
                    "instant_rematch_count": features.get("instant_rematch_count", 0) + 5
                })
                premium_expiry = new_expiry
            else:
                if feature == "instant_rematch":
                    features["instant_rematch_count"] = features.get("instant_rematch_count", 0) + 1
                elif feature == "vaulted_chats":
                    features["vaulted_chats"] = True
                else:
                    features[feature] = current_time + duration
                if duration and (not premium_expiry or premium_expiry < current_time + duration):
                    premium_expiry = current_time + duration
            # Use transaction for critical update
            if PYMONGO_AVAILABLE and db is not None:
                with db.client.start_session() as session:
                    with session.start_transaction():
                        try:
                            users = get_db_collection("users")
                            result = users.update_one(
                                {"user_id": user_id},
                                {"$set": {
                                    "premium_expiry": premium_expiry,
                                    "premium_features": features,
                                    "profile": profile,
                                    "consent": user.get("consent", False),
                                    "verified": user.get("verified", False),
                                    "created_at": user.get("created_at", int(time.time()))
                                }},
                                upsert=True,
                                session=session
                            )
                            if result.matched_count == 0 and result.upserted_id is None:
                                logger.error(f"Failed to save purchase for user {user_id}: {feature}")
                                safe_reply(update, "❌ Error processing your purchase. Please contact support.")
                                send_channel_notification(context.bot, f"🚨 *Payment Error*: Failed to save purchase for user {user_id}: {feature}")
                                return
                            user_cache[user_id] = {**user_cache.get(user_id, {}), **{
                                "premium_expiry": premium_expiry,
                                "premium_features": features,
                                "profile": profile,
                                "consent": user.get("consent", False),
                                "verified": user.get("verified", False),
                                "created_at": user.get("created_at", int(time.time()))
                            }}
                        except pymongo.errors.PyMongoError as e:
                            logger.error(f"Transaction failed for user {user_id}: {e}")
                            session.abort_transaction()
                            safe_reply(update, "❌ Error processing your purchase. Please contact support.")
                            send_channel_notification(context.bot, f"🚨 *Payment Error*: MongoDB transaction failed for user {user_id}: {e}")
                            return
            else:
                if not update_user(user_id, {
                    "premium_expiry": premium_expiry,
                    "premium_features": features,
                    "profile": profile,
                    "consent": user.get("consent", False),
                    "verified": user.get("verified", False),
                    "created_at": user.get("created_at", int(time.time()))
                }):
                    logger.error(f"Failed to save purchase for user {user_id}: {feature}")
                    safe_reply(update, "❌ Error processing your purchase. Please contact support.")
                    send_channel_notification(context.bot, f"🚨 *Payment Error*: Failed to save purchase for user {user_id}: {feature}")
                    return
            safe_reply(update, message)
            expiry_date = (
                datetime.fromtimestamp(current_time + duration).strftime("%Y-%m-%d %H:%M:%S")
                if duration else "No expiry"
            )
            notification_message = (
                "🌟 *New Premium Purchase* 🌟\n\n"
                f"👤 *User ID*: {user_id}\n"
                f"🧑 *Name*: {profile.get('name', 'Not set')}\n"
                f"🎂 *Age*: {profile.get('age', 'Not set')}\n"
                f"👤 *Gender*: {profile.get('gender', 'Not set')}\n"
                f"📍 *Location*: {profile.get('location', 'Not set')}\n"
                f"✨ *Feature*: {feature_name}\n"
                f"💰 *Cost*: {stars} Stars\n"
                f"📅 *Expiry*: {expiry_date}\n"
                f"🕒 *Purchased*: {datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S')}"
            )
            send_channel_notification(context.bot, notification_message)
            logger.info(f"User {user_id} purchased {feature} with Stars")
            break
    else:
        logger.warning(f"Unknown payload for user {user_id}: {payload}")
        safe_reply(update, "❌ Unknown purchase error. Please contact support.")

def shine(update: Update, context: CallbackContext) -> None:
    """Prioritize user in the match queue (premium feature)"""
    user_id = update.effective_user.id
    if not check_rate_limit(user_id):
        safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again.", parse_mode="MarkdownV2")
        return
    if is_banned(user_id):
        user = get_user(user_id)
        ban_type = user.get("ban_type", "temporary")
        ban_expiry = user.get("ban_expiry")
        ban_msg = (
            "🚫 You are permanently banned\\. Contact support to appeal\\."
            if ban_type == "permanent" else
            f"🚫 You are banned until {escape_markdown_v2(datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M'))}\\."
        )
        safe_reply(update, ban_msg, parse_mode="MarkdownV2")
        return
    if not has_premium_feature(user_id, "shine_profile"):
        safe_reply(update, "🌟 *Shine Profile* is a premium feature\\. Buy it with /premium\\!", parse_mode="MarkdownV2")
        return
    with waiting_users_lock:
        if user_id in waiting_users or user_id in user_pairs:
            safe_reply(update, "❓ You're already in a chat or waiting list\\.", parse_mode="MarkdownV2")
            return
        waiting_users.insert(0, user_id)
    safe_reply(update, "✨ Your profile is now shining\\! You’re first in line for matches\\!", parse_mode="MarkdownV2")
    logger.info(f"User {user_id} used shine_profile")
    match_users(context)

def instant(update: Update, context: CallbackContext) -> None:
    """Instantly rematch with a past partner (premium feature)"""
    user_id = update.effective_user.id
    if not check_rate_limit(user_id):
        safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again.", parse_mode="MarkdownV2")
        return
    if is_banned(user_id):
        user = get_user(user_id)
        ban_type = user.get("ban_type", "temporary")
        ban_expiry = user.get("ban_expiry")
        ban_msg = (
            "🚫 You are permanently banned\\. Contact support to appeal\\."
            if ban_type == "permanent" else
            f"🚫 You are banned until {escape_markdown_v2(datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M'))}\\."
        )
        safe_reply(update, ban_msg, parse_mode="MarkdownV2")
        return
    if not has_premium_feature(user_id, "instant_rematch"):
        safe_reply(update, "🔄 *Instant Rematch* is a premium feature\\. Buy it with /premium\\!", parse_mode="MarkdownV2")
        return
    user = get_user(user_id)
    features = user.get("premium_features", {})
    rematch_count = features.get("instant_rematch_count", 0)
    if rematch_count <= 0:
        safe_reply(update, "🔄 You need an *Instant Rematch*\\. Buy one with /premium\\!", parse_mode="MarkdownV2")
        return
    partners = user.get("profile", {}).get("past_partners", [])
    if not partners:
        safe_reply(update, "❌ No past partners to rematch with\\.", parse_mode="MarkdownV2")
        return
    partner_id = partners[-1]  # Use most recent partner
    partner_data = get_user(partner_id)
    if not partner_data:
        safe_reply(update, "❌ Your previous partner is no longer available\\.", parse_mode="MarkdownV2")
        return
    with user_pairs_lock:
        if user_id in user_pairs:
            safe_reply(update, "❓ You're already in a chat\\. Use /stop to end it first\\.", parse_mode="MarkdownV2")
            return
        if partner_id in user_pairs:
            safe_reply(update, "❌ Your previous partner is currently in another chat\\.", parse_mode="MarkdownV2")
            return
        if partner_id in waiting_users:
            with waiting_users_lock:
                waiting_users.remove(partner_id)
            user_pairs[user_id] = partner_id
            user_pairs[partner_id] = user_id
            features["instant_rematch_count"] = rematch_count - 1
            try:
                if not update_user(user_id, {
                    "premium_features": features,
                    "premium_expiry": user.get("premium_expiry"),
                    "profile": user.get("profile", {}),
                    "consent": user.get("consent", False),
                    "verified": user.get("verified", False),
                    "created_at": user.get("created_at", int(time.time())),
                    "ban_type": user.get("ban_type"),
                    "ban_expiry": user.get("ban_expiry")
                }):
                    logger.error(f"Failed to update instant_rematch_count for user {user_id}")
                    safe_reply(update, "❌ Error processing rematch\\. Please try again\\.", parse_mode="MarkdownV2")
                    return
            except PyMongoError as e:
                logger.error(f"MongoDB error updating user {user_id}: {e}")
                safe_reply(update, "❌ Database error\\. Please try again\\.", parse_mode="MarkdownV2")
                return
            with chat_histories_lock:
                if has_premium_feature(user_id, "vaulted_chats"):
                    chat_histories[user_id] = chat_histories.get(user_id, [])
                if has_premium_feature(partner_id, "vaulted_chats"):
                    chat_histories[partner_id] = chat_histories.get(partner_id, [])
            safe_reply(update, "🔄 *Instantly reconnected\\!* Start chatting\\! 🗣️", parse_mode="MarkdownV2")
            safe_bot_send_message(context.bot, partner_id, "🔄 *Instantly reconnected\\!* Start chatting\\! 🗣️", parse_mode="MarkdownV2")
            logger.info(f"User {user_id} used instant_rematch with {partner_id}")
            return
    # Partner not in waiting_users, send rematch request
    keyboard = [
        [InlineKeyboardButton("✅ Accept", callback_data=f"rematch_accept_{user_id}"),
         InlineKeyboardButton("❌ Decline", callback_data="rematch_decline")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    user_profile = user.get("profile", {})
    request_message = (
        f"🔄 *Rematch Request* 🔄\n\n"
        f"A user wants to reconnect with you\\!\n"
        f"🧑 *Name*: {escape_markdown_v2(user_profile.get('name', 'Anonymous'))}\n"
        f"🎂 *Age*: {escape_markdown_v2(str(user_profile.get('age', 'Not set')))}\n"
        f"👤 *Gender*: {escape_markdown_v2(user_profile.get('gender', 'Not set'))}\n"
        f"📍 *Location*: {escape_markdown_v2(user_profile.get('location', 'Not set'))}\n\n"
        f"Would you like to chat again\\?"
    )
    try:
        message = context.bot.send_message(
            chat_id=partner_id,
            text=request_message,
            parse_mode="MarkdownV2",
            reply_markup=reply_markup
        )
        safe_reply(update, "📩 Rematch request sent to your previous partner\\. Waiting for their response\\...", parse_mode="MarkdownV2")
        context.bot_data["rematch_requests"] = context.bot_data.get("rematch_requests", {})
        context.bot_data["rematch_requests"][partner_id] = {
            "requester_id": user_id,
            "timestamp": int(time.time()),
            "message_id": message.message_id
        }
        logger.info(f"User {user_id} sent rematch request to {partner_id}")
    except TelegramError as e:
        safe_reply(update, "❌ Unable to reach your previous partner\\. They may be offline\\.", parse_mode="MarkdownV2")
        logger.warning(f"Failed to send rematch request to {partner_id}: {e}")

def mood(update: Update, context: CallbackContext) -> None:
    """Display mood selection menu (premium feature)"""
    user_id = update.effective_user.id
    if not check_rate_limit(user_id):
        safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again.", parse_mode="MarkdownV2")
        return
    if is_banned(user_id):
        user = get_user(user_id)
        ban_type = user.get("ban_type", "temporary")
        ban_expiry = user.get("ban_expiry")
        ban_msg = (
            "🚫 You are permanently banned\\. Contact support to appeal\\."
            if ban_type == "permanent" else
            f"🚫 You are banned until {escape_markdown_v2(datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M'))}\\."
        )
        safe_reply(update, ban_msg, parse_mode="MarkdownV2")
        return
    if not has_premium_feature(user_id, "mood_match"):
        safe_reply(update, "😊 *Mood Match* is a premium feature\\. Buy it with /premium\\!", parse_mode="MarkdownV2")
        return
    keyboard = [
        [InlineKeyboardButton("😎 Chill", callback_data="mood_chill"),
         InlineKeyboardButton("🤔 Deep", callback_data="mood_deep")],
        [InlineKeyboardButton("😂 Fun", callback_data="mood_fun"),
         InlineKeyboardButton("❌ Clear Mood", callback_data="mood_clear")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    safe_reply(update, "🎭 Choose your chat mood\\:", reply_markup=reply_markup, parse_mode="MarkdownV2")
    logger.info(f"User {user_id} opened mood selection menu")

def set_mood(update: Update, context: CallbackContext) -> None:
    """Set user's chat mood (premium feature)"""
    query = update.callback_query
    user_id = query.from_user.id
    if not check_rate_limit(user_id):
        query.answer()
        safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again.", parse_mode="MarkdownV2")
        return
    if not has_premium_feature(user_id, "mood_match"):
        query.answer()
        safe_reply(update, "😊 *Mood Match* is a premium feature\\. Buy it with /premium\\!", parse_mode="MarkdownV2")
        return
    choice = query.data
    user = get_user(user_id)
    profile = user.get("profile", {})
    try:
        query.answer()
        if choice == "mood_clear":
            profile.pop("mood", None)
            safe_reply(update, "❌ Mood cleared successfully\\.", parse_mode="MarkdownV2")
        else:
            mood = choice.split("_")[1].capitalize()
            profile["mood"] = mood
            safe_reply(update, f"🎭 Mood set to: *{escape_markdown_v2(mood)}*\\!", parse_mode="MarkdownV2")
        try:
            if not update_user(user_id, {
                "profile": profile,
                "premium_expiry": user.get("premium_expiry"),
                "premium_features": user.get("premium_features", {}),
                "consent": user.get("consent", False),
                "verified": user.get("verified", False),
                "created_at": user.get("created_at", int(time.time())),
                "ban_type": user.get("ban_type"),
                "ban_expiry": user.get("ban_expiry")
            }):
                logger.error(f"Failed to update mood for user {user_id}")
                safe_reply(update, "❌ Error setting mood\\. Please try again\\.", parse_mode="MarkdownV2")
                return
        except PyMongoError as e:
            logger.error(f"MongoDB error setting mood for user {user_id}: {e}")
            safe_reply(update, "❌ Database error\\. Please try again\\.", parse_mode="MarkdownV2")
            return
        logger.info(f"User {user_id} set mood to {choice}")
    except Exception as e:
        logger.error(f"Error in set_mood for user {user_id}: {e}")
        safe_reply(update, "❌ Error processing mood selection\\. Please try again\\.", parse_mode="MarkdownV2")

def vault(update: Update, context: CallbackContext) -> None:
    """Enable chat history saving for current chat (premium feature)"""
    user_id = update.effective_user.id
    if not check_rate_limit(user_id):
        safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again.", parse_mode="MarkdownV2")
        return
    if is_banned(user_id):
        user = get_user(user_id)
        ban_type = user.get("ban_type", "temporary")
        ban_expiry = user.get("ban_expiry")
        ban_msg = (
            "🚫 You are permanently banned\\. Contact support to appeal\\."
            if ban_type == "permanent" else
            f"🚫 You are banned until {escape_markdown_v2(datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M'))}\\."
        )
        safe_reply(update, ban_msg, parse_mode="MarkdownV2")
        return
    if not has_premium_feature(user_id, "vaulted_chats"):
        safe_reply(update, "📜 *Vaulted Chats* is a premium feature\\. Buy it with /premium\\!", parse_mode="MarkdownV2")
        return
    with user_pairs_lock:
        if user_id not in user_pairs:
            safe_reply(update, "❓ You're not in a chat\\. Use /start to begin\\.", parse_mode="MarkdownV2")
            return
    with chat_histories_lock:
        chat_histories[user_id] = chat_histories.get(user_id, [])
    safe_reply(update, "📜 Your current chat is being saved to the vault\\!", parse_mode="MarkdownV2")
    logger.info(f"User {user_id} enabled vaulted chats")

def history(update: Update, context: CallbackContext) -> None:
    """Display chat history for premium users"""
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_type = user.get("ban_type", "temporary")
        ban_expiry = user.get("ban_expiry")
        ban_msg = (
            "🚫 You are permanently banned\\. Contact support to appeal\\."
            if ban_type == "permanent" else
            f"🚫 You are banned until {escape_markdown_v2(datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M'))}\\."
        )
        safe_reply(update, ban_msg, parse_mode="MarkdownV2")
        return
    if not has_premium_feature(user_id, "vaulted_chats"):
        safe_reply(update, "📜 *Chat History* is a premium feature\\. Buy it with /premium\\!", parse_mode="MarkdownV2")
        return
    with chat_histories_lock:
        history = chat_histories.get(user_id, [])
    if not history:
        safe_reply(update, "📭 Your chat vault is empty\\.", parse_mode="MarkdownV2")
        return
    history_text = "📜 *Your Chat History* 📜\n\n"
    for idx, entry in enumerate(history[:10], 1):  # Limit to 10 recent messages
        partner_name = get_user(entry.get("partner_id", 0)).get("profile", {}).get("name", "Anonymous") if entry.get("partner_id") else "Unknown"
        timestamp = datetime.fromtimestamp(entry.get("timestamp", 0)).strftime("%Y-%m-%d %H:%M") if entry.get("timestamp") else "Unknown"
        history_text += (
            f"*{idx}*\\. [{escape_markdown_v2(timestamp)}] "
            f"{escape_markdown_v2(partner_name)}: {escape_markdown_v2(entry.get('message', 'No message'))}\n"
        )
    safe_reply(update, history_text, parse_mode="MarkdownV2")
    logger.info(f"User {user_id} viewed chat history")

def rematch(update: Update, context: CallbackContext) -> None:
    """Initiate a rematch with past partners"""
    user_id = update.effective_user.id
    if not check_rate_limit(user_id):
        safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again.", parse_mode="MarkdownV2")
        return
    if is_banned(user_id):
        user = get_user(user_id)
        ban_type = user.get("ban_type", "temporary")
        ban_expiry = user.get("ban_expiry")
        ban_msg = (
            "🚫 You are permanently banned\\. Contact support to appeal\\."
            if ban_type == "permanent" else
            f"🚫 You are banned until {escape_markdown_v2(datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M'))}\\."
        )
        safe_reply(update, ban_msg, parse_mode="MarkdownV2")
        return
    if not has_premium_feature(user_id, "instant_rematch"):
        safe_reply(update, "🔄 *Rematch* is a premium feature\\. Buy it with /premium\\!", parse_mode="MarkdownV2")
        return
    user = get_user(user_id)
    partners = user.get("profile", {}).get("past_partners", [])
    if not partners:
        safe_reply(update, "❌ No past partners to rematch with\\.", parse_mode="MarkdownV2")
        return
    keyboard = []
    for partner_id in partners[-5:]:  # Limit to last 5 partners
        partner_data = get_user(partner_id)
        if partner_data:
            partner_name = partner_data.get("profile", {}).get("name", "Anonymous")
            keyboard.append([
                InlineKeyboardButton(
                    f"Reconnect with {partner_name}",
                    callback_data=f"rematch_request_{partner_id}"
                )
            ])
    if not keyboard:
        safe_reply(update, "❌ No available past partners to rematch with\\.", parse_mode="MarkdownV2")
        return
    reply_markup = InlineKeyboardMarkup(keyboard)
    safe_reply(update, "🔄 *Choose a Past Partner to Rematch* 🔄", reply_markup=reply_markup, parse_mode="MarkdownV2")
    logger.info(f"User {user_id} opened rematch menu")

def flare(update: Update, context: CallbackContext) -> None:
    """Activate flare messages for premium users"""
    user_id = update.effective_user.id
    if not check_rate_limit(user_id):
        safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again.", parse_mode="MarkdownV2")
        return
    if is_banned(user_id):
        user = get_user(user_id)
        ban_type = user.get("ban_type", "temporary")
        ban_expiry = user.get("ban_expiry")
        ban_msg = (
            "🚫 You are permanently banned\\. Contact support to appeal\\."
            if ban_type == "permanent" else
            f"🚫 You are banned until {escape_markdown_v2(datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M'))}\\."
        )
        safe_reply(update, ban_msg, parse_mode="MarkdownV2")
        return
    if not has_premium_feature(user_id, "flare_messages"):
        safe_reply(update, "🌟 *Flare Messages* is a premium feature\\. Buy it with /premium\\!", parse_mode="MarkdownV2")
        return
    safe_reply(update, "✨ Your messages are sparkling with *Flare*\\! Keep chatting to show it off\\!", parse_mode="MarkdownV2")
    logger.info(f"User {user_id} activated flare messages")

def settings(update: Update, context: CallbackContext) -> int:
    """Display settings menu for profile customization"""
    user_id = update.effective_user.id
    logger.info(f"Settings called for user {user_id}")
    try:
        user = get_user(user_id)
        if not user:
            logger.error(f"No user data for user_id={user_id}")
            safe_reply(update, "⚠️ User data not found\\. Please restart with /start\\.", parse_mode="MarkdownV2")
            return ConversationHandler.END
        if is_banned(user_id):
            ban_type = user.get("ban_type", "temporary")
            ban_expiry = user.get("ban_expiry")
            ban_msg = (
                "🚫 You are permanently banned\\. Contact support to appeal\\."
                if ban_type == "permanent" else
                f"🚫 You are banned until {escape_markdown_v2(datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M'))}\\."
            )
            safe_reply(update, ban_msg, parse_mode="MarkdownV2")
            return ConversationHandler.END
        profile = user.get("profile", {})
        keyboard = [
            [
                InlineKeyboardButton("🧑 Change Name", callback_data="set_name"),
                InlineKeyboardButton("🎂 Change Age", callback_data="set_age"),
            ],
            [
                InlineKeyboardButton("👤 Change Gender", callback_data="set_gender"),
                InlineKeyboardButton("📍 Change Location", callback_data="set_location"),
            ],
            [
                InlineKeyboardButton("🏷️ Set Tags", callback_data="set_tags"),
                InlineKeyboardButton("🔙 Back to Help", callback_data="help_menu"),
            ],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        settings_text = (
            "⚙️ *Settings Menu* ⚙️\n\n"
            "Customize your profile to enhance your chat experience\\:\n\n"
            f"🧑 *Name*: {escape_markdown_v2(profile.get('name', 'Not set'))}\n"
            f"🎂 *Age*: {escape_markdown_v2(str(profile.get('age', 'Not set')))}\n"
            f"👤 *Gender*: {escape_markdown_v2(profile.get('gender', 'Not set'))}\n"
            f"📍 *Location*: {escape_markdown_v2(profile.get('location', 'Not set'))}\n"
            f"🏷️ *Tags*: {escape_markdown_v2(', '.join(profile.get('tags', []) or ['None']))}\n\n"
            "Use the buttons below to update your profile\\! 👇"
        )
        safe_reply(update, settings_text, reply_markup=reply_markup, parse_mode="MarkdownV2")
        context.user_data["state"] = SETTINGS
        try:
            if not update_user(user_id, {"setup_state": "SETTINGS"}):
                logger.error(f"Failed to update setup_state for user {user_id}")
                safe_reply(update, "⚠️ Failed to save settings state\\. Please try again\\.", parse_mode="MarkdownV2")
                return ConversationHandler.END
        except PyMongoError as e:
            logger.error(f"MongoDB error updating setup_state for user {user_id}: {e}")
            safe_reply(update, "❌ Database error\\. Please try again\\.", parse_mode="MarkdownV2")
            return ConversationHandler.END
        logger.info(f"User {user_id} opened settings menu, set state to SETTINGS")
        return SETTINGS
    except Exception as e:
        logger.error(f"Error in settings for user {user_id}: {e}", exc_info=True)
        safe_reply(update, "😔 An error occurred\\. Please try again or use /start\\.", parse_mode="MarkdownV2")
        return ConversationHandler.END

def report(update: Update, context: CallbackContext) -> None:
    """Report a user for inappropriate behavior"""
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_type = user.get("ban_type", "temporary")
        ban_expiry = user.get("ban_expiry")
        ban_msg = (
            "🚫 You are permanently banned\\. Contact support to appeal\\."
            if ban_type == "permanent" else
            f"🚫 You are banned until {escape_markdown_v2(datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M'))}\\."
        )
        safe_reply(update, ban_msg, parse_mode="MarkdownV2")
        return
    with user_pairs_lock:
        if user_id not in user_pairs:
            safe_reply(update, "❓ You're not in a chat\\. Use /start to begin\\.", parse_mode="MarkdownV2")
            return
        partner_id = user_pairs[user_id]
    try:
        reports = get_db_collection("reports")
        existing = reports.find_one({"reporter_id": user_id, "reported_id": partner_id})
        if existing:
            safe_reply(update, "⚠️ You've already reported this user\\.", parse_mode="MarkdownV2")
            return
        reports.insert_one({
            "reporter_id": user_id,
            "reported_id": partner_id,
            "timestamp": int(time.time())
        })
        report_count = reports.count_documents({"reported_id": partner_id})
        if report_count >= REPORT_THRESHOLD:
            ban_expiry = int(time.time()) + TEMP_BAN_DURATION
            try:
                if not update_user(partner_id, {
                    "ban_type": "temporary",
                    "ban_expiry": ban_expiry,
                    "profile": get_user(partner_id).get("profile", {}),
                    "consent": get_user(partner_id).get("consent", False),
                    "verified": get_user(partner_id).get("verified", False),
                    "premium_expiry": get_user(partner_id).get("premium_expiry"),
                    "premium_features": get_user(partner_id).get("premium_features", {}),
                    "created_at": get_user(partner_id).get("created_at", int(time.time()))
                }):
                    logger.error(f"Failed to ban user {partner_id}")
                    safe_reply(update, "❌ Error processing report\\. Please contact support\\.", parse_mode="MarkdownV2")
                    return
                violations = get_db_collection("keyword_violations")
                violations.update_one(
                    {"user_id": partner_id},
                    {"$set": {
                        "ban_type": "temporary",
                        "ban_expiry": ban_expiry,
                        "last_violation": int(time.time()),
                        "count": 3,
                        "keyword": "reported"
                    }},
                    upsert=True
                )
            except PyMongoError as e:
                logger.error(f"MongoDB error banning user {partner_id}: {e}")
                safe_reply(update, "❌ Database error\\. Please try again\\.", parse_mode="MarkdownV2")
                return
            safe_bot_send_message(
                context.bot, partner_id,
                f"🚫 *Temporary Ban* 🚫\nYou've been banned for 24 hours due to multiple reports\\. Contact support if you believe this is an error\\.",
                parse_mode="MarkdownV2"
            )
            notification_message = (
                "🚨 *User Banned* 🚨\n\n"
                f"👤 *User ID*: {escape_markdown_v2(str(partner_id))}\n"
                f"📅 *Ban Duration*: 24 hours\n"
                f"🕒 *Ban Expiry*: {escape_markdown_v2(datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M:%S'))}\n"
                f"📢 *Reason*: Multiple reports \\({escape_markdown_v2(str(report_count))}\\)\n"
                f"🕒 *Reported At*: {escape_markdown_v2(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}"
            )
            send_channel_notification(context.bot, notification_message, parse_mode="MarkdownV2")
            logger.info(f"User {partner_id} banned for 24 hours due to {report_count} reports")
            stop(update, context)
        else:
            safe_reply(update, "🚨 Report submitted\\. Thank you for keeping the community safe\\!", parse_mode="MarkdownV2")
            notification_message = (
                "🚨 *New Report Filed* 🚨\n\n"
                f"👤 *Reporter ID*: {escape_markdown_v2(str(user_id))}\n"
                f"👤 *Reported ID*: {escape_markdown_v2(str(partner_id))}\n"
                f"📢 *Total Reports*: {escape_markdown_v2(str(report_count))}\n"
                f"🕒 *Reported At*: {escape_markdown_v2(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}"
            )
            send_channel_notification(context.bot, notification_message, parse_mode="MarkdownV2")
            logger.info(f"User {user_id} reported user {partner_id}. Total reports: {report_count}")
    except PyMongoError as e:
        logger.error(f"MongoDB error processing report from {user_id} against {partner_id}: {e}")
        safe_reply(update, "❌ Database error submitting report\\. Please try again\\.", parse_mode="MarkdownV2")
    except Exception as e:
        logger.error(f"Error processing report from {user_id} against {partner_id}: {e}")
        safe_reply(update, "❌ Error submitting report\\. Please try again\\.", parse_mode="MarkdownV2")

def delete_profile(update: Update, context: CallbackContext) -> None:
    """Delete user profile and all associated data"""
    user_id = update.effective_user.id
    with user_pairs_lock:
        if user_id in user_pairs:
            partner_id = user_pairs[user_id]
            del user_pairs[user_id]
            if partner_id in user_pairs:
                del user_pairs[partner_id]
            safe_bot_send_message(
                context.bot, partner_id,
                "👋 Your partner has left the chat\\. Use /start to find a new one\\.", parse_mode="MarkdownV2"
            )
    with waiting_users_lock:
        if user_id in waiting_users:
            waiting_users.remove(user_id)
    with chat_histories_lock:
        if user_id in chat_histories:
            del chat_histories[user_id]
    try:
        delete_user(user_id)
        safe_reply(update, "🗑️ Your profile and data have been deleted successfully\\.", parse_mode="MarkdownV2")
        notification_message = (
            "🗑️ *User Deleted Profile* 🗑️\n\n"
            f"👤 *User ID*: {escape_markdown_v2(str(user_id))}\n"
            f"🕒 *Deleted At*: {escape_markdown_v2(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}"
        )
        send_channel_notification(context.bot, notification_message, parse_mode="MarkdownV2")
        logger.info(f"User {user_id} deleted their profile")
    except PyMongoError as e:
        logger.error(f"MongoDB error deleting profile for user {user_id}: {e}")
        safe_reply(update, "❌ Database error deleting your profile\\. Please try again\\.", parse_mode="MarkdownV2")
    except Exception as e:
        logger.error(f"Error deleting profile for user {user_id}: {e}")
        safe_reply(update, "❌ Error deleting your profile\\. Please try again\\.", parse_mode="MarkdownV2")

def admin_access(update: Update, context: CallbackContext) -> None:
    """Grant admin access and display commands"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔒 Unauthorized\\.", parse_mode="MarkdownV2")
        logger.info(f"Unauthorized admin access attempt by user_id={user_id}")
        return
    access_text = (
        "🌟 *Admin Commands* 🌟\n\n"
        "🚀 *User Management*\n"
        "• /admin_userslist \\- List all users 📋\n"
        "• /admin_premiumuserslist \\- List premium users 💎\n"
        "• /admin_info <user_id> \\- View user details 🕵️\n"
        "• /admin_delete <user_id> \\- Delete a user’s data 🗑️\n"
        "• /admin_premium <user_id> <days> \\- Grant premium status 🎁\n"
        "• /admin_revoke_premium <user_id> \\- Revoke premium status ❌\n"
        "━━━━━━━━━━━━━━\n\n"
        "🛡️ *Ban Management*\n"
        "• /admin_ban <user_id> <days/permanent> \\- Ban a user 🚫\n"
        "• /admin_unban <user_id> \\- Unban a user 🔓\n"
        "• /admin_violations \\- List recent keyword violations ⚠️\n"
        "━━━━━━━━━━━━━━\n\n"
        "📊 *Reports & Stats*\n"
        "• /admin_reports \\- List reported users 🚨\n"
        "• /admin_clear_reports <user_id> \\- Clear reports 🧹\n"
        "• /admin_stats \\- View bot statistics 📈\n"
        "━━━━━━━━━━━━━━\n\n"
        "📢 *Broadcast*\n"
        "• /admin_broadcast <message> \\- Send message to all users 📣\n"
    )
    safe_reply(update, access_text, parse_mode="MarkdownV2")
    logger.info(f"Admin {user_id} accessed admin commands")

def admin_delete(update: Update, context: CallbackContext) -> None:
    """Delete a user's data"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔒 Unauthorized\\.", parse_mode="MarkdownV2")
        return
    try:
        target_id = int(context.args[0])
        try:
            delete_user(target_id)
            with user_pairs_lock:
                if target_id in user_pairs:
                    partner_id = user_pairs[target_id]
                    del user_pairs[target_id]
                    if partner_id in user_pairs:
                        del user_pairs[partner_id]
                    safe_bot_send_message(
                        context.bot, partner_id,
                        "👋 Your partner’s data was deleted by an admin\\. Use /start to find a new one\\.", parse_mode="MarkdownV2"
                    )
            with waiting_users_lock:
                if target_id in waiting_users:
                    waiting_users.remove(target_id)
            with chat_histories_lock:
                if target_id in chat_histories:
                    del chat_histories[target_id]
            safe_reply(update, f"🗑️ User *{escape_markdown_v2(str(target_id))}* data deleted successfully\\.", parse_mode="MarkdownV2")
            logger.info(f"Admin {user_id} deleted user {target_id}")
        except PyMongoError as e:
            logger.error(f"MongoDB error deleting user {target_id}: {e}")
            safe_reply(update, "❌ Database error deleting user\\.", parse_mode="MarkdownV2")
    except (IndexError, ValueError):
        safe_reply(update, "⚠️ Usage: /admin_delete <user_id> 📋\\.", parse_mode="MarkdownV2")

def admin_premium(update: Update, context: CallbackContext) -> None:
    """Grant premium status to a user"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔒 Unauthorized\\.", parse_mode="MarkdownV2")
        return
    try:
        target_id = int(context.args[0])
        days = int(context.args[1])
        if days <= 0:
            raise ValueError("Days must be positive")
        expiry = int(time.time()) + days * 24 * 3600
        user = get_user(target_id)
        if not user:
            safe_reply(update, "😕 User not found\\.", parse_mode="MarkdownV2")
            return
        features = user.get("premium_features", {})
        features.update({
            "flare_messages": expiry,
            "instant_rematch_count": features.get("instant_rematch_count", 0) + 5,
            "shine_profile": expiry,
            "mood_match": expiry,
            "partner_details": expiry,
            "vaulted_chats": expiry
        })
        try:
            if not update_user(target_id, {
                "premium_expiry": expiry,
                "premium_features": features,
                "profile": user.get("profile", {}),
                "consent": user.get("consent", False),
                "verified": user.get("verified", False),
                "ban_type": user.get("ban_type"),
                "ban_expiry": user.get("ban_expiry"),
                "created_at": user.get("created_at", int(time.time()))
            }):
                logger.error(f"Failed to grant premium to user {target_id}")
                safe_reply(update, "❌ Failed to grant premium status\\.", parse_mode="MarkdownV2")
                return
        except PyMongoError as e:
            logger.error(f"MongoDB error granting premium to user {target_id}: {e}")
            safe_reply(update, "❌ Database error granting premium\\.", parse_mode="MarkdownV2")
            return
        safe_reply(update, f"🎁 Premium granted to user *{escape_markdown_v2(str(target_id))}* for *{escape_markdown_v2(str(days))}* days\\.", parse_mode="MarkdownV2")
        safe_bot_send_message(
            context.bot, target_id,
            f"🎉 You've been granted Premium status for {escape_markdown_v2(str(days))} days\\!", parse_mode="MarkdownV2"
        )
        logger.info(f"Admin {user_id} granted premium to {target_id} for {days} days")
    except (IndexError, ValueError) as e:
        logger.error(f"Invalid input for admin_premium: {e}")
        safe_reply(update, "⚠️ Usage: /admin_premium <user_id> <days> 📋\\.", parse_mode="MarkdownV2")

def admin_revoke_premium(update: Update, context: CallbackContext) -> None:
    """Revoke premium status from a user"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔒 Unauthorized\\.", parse_mode="MarkdownV2")
        return
    try:
        target_id = int(context.args[0])
        user = get_user(target_id)
        if not user:
            safe_reply(update, "😕 User not found\\.", parse_mode="MarkdownV2")
            return
        try:
            if not update_user(target_id, {
                "premium_expiry": None,
                "premium_features": {},
                "profile": user.get("profile", {}),
                "consent": user.get("consent", False),
                "verified": user.get("verified", False),
                "ban_type": user.get("ban_type"),
                "ban_expiry": user.get("ban_expiry"),
                "created_at": user.get("created_at", int(time.time()))
            }):
                logger.error(f"Failed to revoke premium for user {target_id}")
                safe_reply(update, "❌ Failed to revoke premium status\\.", parse_mode="MarkdownV2")
                return
        except PyMongoError as e:
            logger.error(f"MongoDB error revoking premium for user {target_id}: {e}")
            safe_reply(update, "❌ Database error revoking premium\\.", parse_mode="MarkdownV2")
            return
        safe_reply(update, f"❌ Premium status revoked for user *{escape_markdown_v2(str(target_id))}*\\.", parse_mode="MarkdownV2")
        safe_bot_send_message(
            context.bot, target_id,
            "😔 Your Premium status has been revoked\\.", parse_mode="MarkdownV2"
        )
        logger.info(f"Admin {user_id} revoked premium for {target_id}")
    except (IndexError, ValueError):
        safe_reply(update, "⚠️ Usage: /admin_revoke_premium <user_id> 📋\\.", parse_mode="MarkdownV2")

def admin_ban(update: Update, context: CallbackContext) -> None:
    """Ban a user"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔒 Unauthorized\\.", parse_mode="MarkdownV2")
        return
    try:
        target_id = int(context.args[0])
        ban_type = context.args[1].lower()
        user = get_user(target_id)
        if not user:
            safe_reply(update, "😕 User not found\\.", parse_mode="MarkdownV2")
            return
        if ban_type == "permanent":
            ban_expiry = None
        elif ban_type.isdigit():
            days = int(ban_type)
            if days <= 0:
                raise ValueError("Days must be positive")
            ban_expiry = int(time.time()) + days * 24 * 3600
            ban_type = "temporary"
        else:
            raise ValueError("Invalid ban type")
        try:
            if not update_user(target_id, {
                "ban_type": ban_type,
                "ban_expiry": ban_expiry,
                "profile": user.get("profile", {}),
                "consent": user.get("consent", False),
                "verified": user.get("verified", False),
                "premium_expiry": user.get("premium_expiry"),
                "premium_features": user.get("premium_features", {}),
                "created_at": user.get("created_at", int(time.time()))
            }):
                logger.error(f"Failed to ban user {target_id}")
                safe_reply(update, "❌ Failed to ban user\\.", parse_mode="MarkdownV2")
                return
            violations = get_db_collection("keyword_violations")
            violations.update_one(
                {"user_id": target_id},
                {"$set": {
                    "count": 5 if ban_type == "permanent" else 3,
                    "keyword": "admin_ban",
                    "last_violation": int(time.time()),
                    "ban_type": ban_type,
                    "ban_expiry": ban_expiry
                }},
                upsert=True
            )
        except PyMongoError as e:
            logger.error(f"MongoDB error banning user {target_id}: {e}")
            safe_reply(update, "❌ Database error banning user\\.", parse_mode="MarkdownV2")
            return
        with user_pairs_lock:
            if target_id in user_pairs:
                partner_id = user_pairs[target_id]
                del user_pairs[target_id]
                if partner_id in user_pairs:
                    del user_pairs[partner_id]
                safe_bot_send_message(
                    context.bot, partner_id,
                    "😔 Your partner has been banned\\. Use /start to find a new one\\.", parse_mode="MarkdownV2"
                )
        with waiting_users_lock:
            if target_id in waiting_users:
                waiting_users.remove(target_id)
        ban_message = (
            f"🚫 You have been {escape_markdown_v2(ban_type)} banned from Talk2Anyone\\."
            f"{'' if ban_type == 'permanent' else f" Until {escape_markdown_v2(...)}\\.")}"
        )
        safe_bot_send_message(context.bot, target_id, ban_message, parse_mode="MarkdownV2")
        safe_reply(update, f"🚫 User *{escape_markdown_v2(str(target_id))}* has been {escape_markdown_v2(ban_type)} banned\\.", parse_mode="MarkdownV2")
        logger.info(f"Admin {user_id} banned user {target_id} ({ban_type})")
    except (IndexError, ValueError):
        safe_reply(update, "⚠️ Usage: /admin_ban <user_id> <days/permanent> 📋\\.", parse_mode="MarkdownV2")

def admin_unban(update: Update, context: CallbackContext) -> None:
    """Unban a user"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔒 Unauthorized\\.", parse_mode="MarkdownV2")
        return
    try:
        target_id = int(context.args[0])
        user = get_user(target_id)
        if not user:
            safe_reply(update, "😕 User not found\\.", parse_mode="MarkdownV2")
            return
        try:
            if not update_user(target_id, {
                "ban_type": None,
                "ban_expiry": None,
                "profile": user.get("profile", {}),
                "consent": user.get("consent", False),
                "verified": user.get("verified", False),
                "premium_expiry": user.get("premium_expiry"),
                "premium_features": user.get("premium_features", {}),
                "created_at": user.get("created_at", int(time.time()))
            }):
                logger.error(f"Failed to unban user {target_id}")
                safe_reply(update, "❌ Failed to unban user\\.", parse_mode="MarkdownV2")
                return
            violations = get_db_collection("keyword_violations")
            violations.delete_one({"user_id": target_id})
        except PyMongoError as e:
            logger.error(f"MongoDB error unbanning user {target_id}: {e}")
            safe_reply(update, "❌ Database error unbanning user\\.", parse_mode="MarkdownV2")
            return
        safe_reply(update, f"🔓 User *{escape_markdown_v2(str(target_id))}* has been unbanned\\.", parse_mode="MarkdownV2")
        safe_bot_send_message(
            context.bot, target_id,
            "🎉 You have been unbanned\\. Use /start to begin\\.", parse_mode="MarkdownV2"
        )
        logger.info(f"Admin {user_id} unbanned user {target_id}")
    except (IndexError, ValueError):
        safe_reply(update, "⚠️ Usage: /admin_unban <user_id> 📋\\.", parse_mode="MarkdownV2")

def admin_violations(update: Update, context: CallbackContext) -> None:
    """List recent keyword violations"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔒 Unauthorized\\.", parse_mode="MarkdownV2")
        return
    try:
        violations = get_db_collection("keyword_violations")
        cursor = violations.find().sort("last_violation", -1).limit(10)
        violations_list = list(cursor)
        if not violations_list:
            safe_reply(update, "✅ No recent keyword violations\\.", parse_mode="MarkdownV2")
            return
        violation_text = "⚠️ *Recent Keyword Violations* ⚠️\n\n"
        for v in violations_list:
            v_user_id = v["user_id"]
            count = v.get("count", 0)
            keyword = v.get("keyword", "N/A")
            last_violation = datetime.fromtimestamp(v["last_violation"]).strftime('%Y-%m-%d %H:%M') if v.get("last_violation") else "Unknown"
            ban_type = v.get("ban_type")
            ban_expiry = v.get("ban_expiry")
            ban_status = (
                "Permanent 🔒" if ban_type == "permanent" else
                f"Temporary until {escape_markdown_v2(datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M'))} ⏰"
                if ban_type == "temporary" and ban_expiry else "None ✅"
            )
            violation_text += (
                f"👤 *User ID*: {escape_markdown_v2(str(v_user_id))}\n"
                f"📉 *Violations*: {escape_markdown_v2(str(count))}\n"
                f"🔍 *Keyword*: {escape_markdown_v2(keyword)}\n"
                f"🕒 *Last*: {escape_markdown_v2(last_violation)}\n"
                f"🚫 *Ban*: {escape_markdown_v2(ban_status)}\n"
                "━━━━━━━━━━━━━━\n"
            )
        safe_reply(update, violation_text, parse_mode="MarkdownV2")
        logger.info(f"Admin {user_id} viewed recent keyword violations")
    except PyMongoError as e:
        logger.error(f"MongoDB error fetching violations: {e}")
        safe_reply(update, "😔 Database error fetching violations\\.", parse_mode="MarkdownV2")
    except Exception as e:
        logger.error(f"Error fetching violations: {e}")
        safe_reply(update, "😔 Error fetching violations\\.", parse_mode="MarkdownV2")

def admin_userslist(update: Update, context: CallbackContext) -> None:
    """List all users for authorized admins"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔒 Unauthorized\\.", parse_mode="MarkdownV2")
        return
    try:
        users = get_db_collection("users")
        users_list = list(users.find().sort("user_id", 1))
        if not users_list:
            safe_reply(update, "😕 No users found\\.", parse_mode="MarkdownV2")
            logger.info(f"Admin {user_id} requested users list: no users found")
            return
        message = "📋 *All Users List* \\(Sorted by ID\\) 📋\n\n"
        user_count = 0
        for user in users_list:
            u_id = user["user_id"]
            profile = user.get("profile", {})
            premium_features = user.get("premium_features", {})
            created_at = user.get("created_at", int(time.time()))
            created_date = (
                datetime.fromtimestamp(created_at).strftime("%Y-%m-%d")
                if created_at and isinstance(created_at, (int, float))
                else "Unknown"
            )
            has_active_features = any(
                v is True or (isinstance(v, int) and v > time.time())
                for k, v in premium_features.items()
                if k != "instant_rematch_count"
            )
            premium_status = (
                "Premium 💎" if (user.get("premium_expiry") and user["premium_expiry"] > time.time()) or has_active_features else "Not Premium 🌑"
            )
            ban_status = user.get("ban_type", "None")
            verified_status = "Yes ✅" if user.get("verified", False) else "No ❌"
            message += (
                f"👤 *User ID*: {escape_markdown_v2(str(u_id))}\n"
                f"🧑 *Name*: {escape_markdown_v2(profile.get('name', 'Not set'))}\n"
                f"📅 *Created*: {escape_markdown_v2(created_date)}\n"
                f"💎 *Premium*: {escape_markdown_v2(premium_status)}\n"
                f"🚫 *Ban*: {escape_markdown_v2(ban_status)}\n"
                f"✅ *Verified*: {escape_markdown_v2(verified_status)}\n"
                "━━━━━━━━━━━━━━\n"
            )
            user_count += 1
            if len(message.encode('utf-8')) > 3500:
                safe_reply(update, message, parse_mode="MarkdownV2")
                message = ""
        if message.strip():
            message += f"📊 *Total Users*: {escape_markdown_v2(str(user_count))}\n"
            safe_reply(update, message, parse_mode="MarkdownV2")
        logger.info(f"Admin {user_id} requested users list with {user_count} users")
    except PyMongoError as e:
        logger.error(f"MongoDB error fetching users list for admin {user_id}: {e}")
        safe_reply(update, "😔 Database error retrieving users list\\.", parse_mode="MarkdownV2")
    except Exception as e:
        logger.error(f"Error fetching users list for admin {user_id}: {e}")
        safe_reply(update, "😔 Error retrieving users list\\.", parse_mode="MarkdownV2")

def admin_premiumuserslist(update: Update, context: CallbackContext) -> None:
    """List premium users"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔒 Unauthorized\\.", parse_mode="MarkdownV2")
        return
    try:
        current_time = int(time.time())
        users = get_db_collection("users")
        premium_users = list(users.find({"premium_expiry": {"$gt": current_time}}).sort("premium_expiry", -1))
        if not premium_users:
            safe_reply(update, "😕 No premium users found\\.", parse_mode="MarkdownV2")
            return
        message = "💎 *Premium Users List* \\(Sorted by Expiry\\) 💎\n\n"
        user_count = 0
        for user in premium_users:
            u_id = user["user_id"]
            premium_expiry = user.get("premium_expiry")
            profile = user.get("profile", {})
            expiry_date = (
                datetime.fromtimestamp(premium_expiry).strftime("%Y-%m-%d")
                if premium_expiry and isinstance(premium_expiry, (int, float)) and premium_expiry > current_time
                else "No expiry set"
            )
            active_features = [k for k, v in user.get("premium_features", {}).items() if v is True or (isinstance(v, int) and v > current_time)]
            if "instant_rematch_count" in user.get("premium_features", {}) and user["premium_features"]["instant_rematch_count"] > 0:
                active_features.append(f"instant_rematch_count: {user['premium_features']['instant_rematch_count']}")
            message += (
                f"👤 *User ID*: {escape_markdown_v2(str(u_id))}\n"
                f"🧑 *Name*: {escape_markdown_v2(profile.get('name', 'Not set'))}\n"
                f"⏰ *Premium Until*: {escape_markdown_v2(expiry_date)}\n"
                f"✨ *Features*: {escape_markdown_v2(', '.join(active_features) or 'None')}\n"
                "━━━━━━━━━━━━━━\n"
            )
            user_count += 1
            if len(message.encode('utf-8')) > 3500:
                safe_reply(update, message, parse_mode="MarkdownV2")
                message = ""
        if message:
            message += f"📊 *Total Premium Users*: {escape_markdown_v2(str(user_count))}\n"
            safe_reply(update, message, parse_mode="MarkdownV2")
        logger.info(f"Admin {user_id} requested premium users list with {user_count} users")
    except PyMongoError as e:
        logger.error(f"MongoDB error fetching premium users list: {e}")
        safe_reply(update, "😔 Database error retrieving premium users list\\.", parse_mode="MarkdownV2")
    except Exception as e:
        logger.error(f"Error fetching premium users list: {e}")
        safe_reply(update, "😔 Error retrieving premium users list\\.", parse_mode="MarkdownV2")

def admin_info(update: Update, context: CallbackContext) -> None:
    """Display detailed user information"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔒 Unauthorized\\.", parse_mode="MarkdownV2")
        return
    try:
        target_id = int(context.args[0])
        user = get_user(target_id)
        if not user:
            safe_reply(update, "😕 User not found\\.", parse_mode="MarkdownV2")
            return
        profile = user.get("profile", {})
        consent = "Yes ✅" if user.get("consent") else "No ❌"
        verified = "Yes ✅" if user.get("verified") else "No ❌"
        premium = user.get("premium_expiry")
        premium_status = (
            f"Until {escape_markdown_v2(datetime.fromtimestamp(premium).strftime('%Y-%m-%d %H:%M:%S'))} ⏰"
            if premium and premium > time.time() else "None 🌑"
        )
        ban_status = user.get("ban_type")
        ban_info = (
            "Permanent 🔒" if ban_status == "permanent" else
            f"Until {escape_markdown_v2(datetime.fromtimestamp(user.get('ban_expiry')).strftime('%Y-%m-%d %H:%M:%S'))} ⏰"
            if ban_status == "temporary" and user.get("ban_expiry") > time.time() else "None ✅"
        )
        created_at = datetime.fromtimestamp(user.get("created_at", int(time.time()))).strftime("%Y-%m-%d %H:%M:%S")
        features = ", ".join([k for k, v in user.get("premium_features", {}).items() if v is True or (isinstance(v, int) and v > time.time())]) or "None"
        violations = get_db_collection("keyword_violations").find_one({"user_id": target_id})
        violations_count = violations.get("count", 0) if violations else 0
        violation_status = (
            "Permanent 🔒" if violations and violations.get("ban_type") == "permanent" else
            f"Temporary until {escape_markdown_v2(datetime.fromtimestamp(violations['ban_expiry']).strftime('%Y-%m-%d %H:%M'))} ⏰"
            if violations and violations.get("ban_type") == "temporary" and violations.get("ban_expiry") else
            f"{escape_markdown_v2(str(violations_count))} warnings ⚠️" if violations_count > 0 else "None ✅"
        )
        message = (
            f"🕵️ *User Info: {escape_markdown_v2(str(target_id))}* 🕵️\n\n"
            f"🧑 *Name*: {escape_markdown_v2(profile.get('name', 'Not set'))}\n"
            f"🎂 *Age*: {escape_markdown_v2(str(profile.get('age', 'Not set')))}\n"
            f"👤 *Gender*: {escape_markdown_v2(profile.get('gender', 'Not set'))}\n"
            f"📍 *Location*: {escape_markdown_v2(profile.get('location', 'Not set'))}\n"
            f"🏷️ *Tags*: {escape_markdown_v2(', '.join(profile.get('tags', []) or ['None']))}\n"
            f"🤝 *Consent*: {escape_markdown_v2(consent)}\n"
            f"✅ *Verified*: {escape_markdown_v2(verified)}\n"
            f"💎 *Premium*: {escape_markdown_v2(premium_status)}\n"
            f"✨ *Features*: {escape_markdown_v2(features)}\n"
            f"🚫 *Ban*: {escape_markdown_v2(ban_info)}\n"
            f"⚠️ *Keyword Violations*: {escape_markdown_v2(violation_status)}\n"
            f"📅 *Joined*: {escape_markdown_v2(created_at)}"
        )
        safe_reply(update, message, parse_mode="MarkdownV2")
        logger.info(f"Admin {user_id} viewed info for user {target_id}")
    except (IndexError, ValueError):
        safe_reply(update, "⚠️ Usage: /admin_info <user_id> 📋\\.", parse_mode="MarkdownV2")

def admin_reports(update: Update, context: CallbackContext) -> None:
    """List reported users"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔒 Unauthorized\\.", parse_mode="MarkdownV2")
        return
    try:
        reports = get_db_collection("reports")
        pipeline = [
            {"$group": {"_id": "$reported_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 20}
        ]
        reports_list = list(reports.aggregate(pipeline))
        if not reports_list:
            safe_reply(update, "✅ No reports found\\.", parse_mode="MarkdownV2")
            return
        message = "🚨 *Reported Users* \\(Top 20\\) 🚨\n\n"
        for report in reports_list:
            reported_id = report["_id"]
            count = report["count"]
            message += f"👤 {escape_markdown_v2(str(reported_id))} | Reports: *{escape_markdown_v2(str(count))}*\n"
        safe_reply(update, message, parse_mode="MarkdownV2")
        logger.info(f"Admin {user_id} viewed reported users")
    except PyMongoError as e:
        logger.error(f"MongoDB error listing reports: {e}")
        safe_reply(update, "😔 Database error retrieving reports\\.", parse_mode="MarkdownV2")
    except Exception as e:
        logger.error(f"Error listing reports: {e}")
        safe_reply(update, "😔 Error retrieving reports\\.", parse_mode="MarkdownV2")

def admin_clear_reports(update: Update, context: CallbackContext) -> None:
    """Clear reports for a user"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔒 Unauthorized\\.", parse_mode="MarkdownV2")
        return
    try:
        target_id = int(context.args[0])
        try:
            reports = get_db_collection("reports")
            reports.delete_many({"reported_id": target_id})
            safe_reply(update, f"🧹 Reports cleared for user *{escape_markdown_v2(str(target_id))}*\\.", parse_mode="MarkdownV2")
            logger.info(f"Admin {user_id} cleared reports for {target_id}")
        except PyMongoError as e:
            logger.error(f"MongoDB error clearing reports for user {target_id}: {e}")
            safe_reply(update, "❌ Database error clearing reports\\.", parse_mode="MarkdownV2")
    except (IndexError, ValueError):
        safe_reply(update, "⚠️ Usage: /admin_clear_reports <user_id> 📋\\.", parse_mode="MarkdownV2")

def admin_broadcast(update: Update, context: CallbackContext) -> None:
    """Broadcast a message to all users"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔒 Unauthorized\\.", parse_mode="MarkdownV2")
        return
    if not context.args:
        safe_reply(update, "⚠️ Usage: /admin_broadcast <message> 📋\\.", parse_mode="MarkdownV2")
        return
    message = "📣 *Announcement*: " + " ".join(context.args)
    try:
        users = get_db_collection("users")
        users_list = users.find({"consent": True})
        sent_count = 0
        for user in users_list:
            try:
                safe_bot_send_message(context.bot, user["user_id"], message, parse_mode="MarkdownV2")
                sent_count += 1
            except TelegramError as e:
                logger.warning(f"Failed to send broadcast to {user['user_id']}: {e}")
        safe_reply(update, f"📢 Broadcast sent to *{escape_markdown_v2(str(sent_count))}* users\\.", parse_mode="MarkdownV2")
        logger.info(f"Admin {user_id} sent broadcast to {sent_count} users")
    except PyMongoError as e:
        logger.error(f"MongoDB error sending broadcast: {e}")
        safe_reply(update, "😔 Database error sending broadcast\\.", parse_mode="MarkdownV2")
    except Exception as e:
        logger.error(f"Error sending broadcast: {e}")
        safe_reply(update, "😔 Error sending broadcast\\.", parse_mode="MarkdownV2")

def admin_stats(update: Update, context: CallbackContext) -> None:
    """Display bot statistics"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔒 Unauthorized\\.", parse_mode="MarkdownV2")
        return
    try:
        users = get_db_collection("users")
        total_users = users.count_documents({})
        current_time = int(time.time())
        premium_users = users.count_documents({"premium_expiry": {"$gt": current_time}})
        banned_users = users.count_documents({
            "ban_type": {"$in": ["permanent", "temporary"]},
            "$or": [{"ban_expiry": {"$gt": current_time}}, {"ban_type": "permanent"}]
        })
        with user_pairs_lock:
            with waiting_users_lock:
                active_users = len(set(user_pairs.keys()).union(waiting_users))
        timestamp = datetime.now().strftime("%Y\\-%m\\-%d %H\\:%M\\:%S")
        stats_message = (
            "📈 *Bot Statistics* 📈\n\n"
            f"👥 *Total Users*: *{escape_markdown_v2(str(total_users))}*\n"
            f"💎 *Premium Users*: *{escape_markdown_v2(str(premium_users))}*\n"
            f"💬 *Active Users*: *{escape_markdown_v2(str(active_users))}* \\(in chats or waiting\\)\n"
            f"🚫 *Banned Users*: *{escape_markdown_v2(str(banned_users))}*\n"
            "━━━━━━━━━━━━━━\n"
            f"🕒 *Updated*: {escape_markdown_v2(timestamp)}"
        )
        safe_reply(update, stats_message, parse_mode="MarkdownV2")
        logger.info(f"Admin {user_id} requested bot statistics: total={total_users}, premium={premium_users}, active={active_users}, banned={banned_users}")
    except PyMongoError as e:
        logger.error(f"MongoDB error fetching bot statistics: {e}")
        safe_reply(update, "😔 Database error retrieving statistics\\.", parse_mode="MarkdownV2")
    except Exception as e:
        logger.error(f"Error fetching bot statistics: {e}")
        safe_reply(update, "😔 Error retrieving statistics\\.", parse_mode="MarkdownV2")

def set_tags(update: Update, context: CallbackContext) -> int:
    """Set user tags for matching"""
    user_id = update.effective_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    tags_input = update.message.text.strip().lower().split(",")
    tags = [tag.strip() for tag in tags_input if tag.strip() in ALLOWED_TAGS]
    if not tags:
        safe_reply(update, (
            "⚠️ Invalid Tags 😕\n"
            f"Enter valid tags \\(e\\.g\\., {escape_markdown_v2(','.join(ALLOWED_TAGS[:3]))}\\)"
            f"\\. Allowed: {escape_markdown_v2(', '.join(ALLOWED_TAGS))}"
        ), parse_mode="MarkdownV2")
        return TAGS
    if len(tags) > 5:
        safe_reply(update, "⚠️ Too Many Tags 😕\nYou can set up to 5 tags\\.", parse_mode="MarkdownV2")
        return TAGS
    profile["tags"] = tags
    try:
        if not update_user(user_id, {
            "profile": profile,
            "consent": user.get("consent", False),
            "verified": user.get("verified", False),
            "premium_expiry": user.get("premium_expiry"),
            "premium_features": user.get("premium_features", {}),
            "ban_type": user.get("ban_type"),
            "ban_expiry": user.get("ban_expiry"),
            "created_at": user.get("created_at", int(time.time()))
        }):
            logger.error(f"Failed to update tags for user {user_id}")
            safe_reply(update, "😔 Error saving tags\\.", parse_mode="MarkdownV2")
            return TAGS
    except PyMongoError as e:
        logger.error(f"MongoDB error updating tags for user {user_id}: {e}")
        safe_reply(update, "❌ Database error saving tags\\. Please try again\\.", parse_mode="MarkdownV2")
        return TAGS
    safe_reply(update, f"🏷️ Tags set successfully: *{escape_markdown_v2(', '.join(tags))}*\\!", parse_mode="MarkdownV2")
    logger.info(f"User {user_id} set tags: {tags}")
    return ConversationHandler.END

def button(update: Update, context: CallbackContext) -> int:
    """Handle all callback queries"""
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    query.answer()
    user = get_user(user_id)
    if not user:
        safe_reply(update, "⚠️ User data not found\\. Please restart with /start\\.", parse_mode="MarkdownV2")
        return ConversationHandler.END

    # Handle rematch request acceptance
    if data.startswith("rematch_accept_"):
        requester_id = int(data.split("_")[2])
        with user_pairs_lock:
            if user_id in user_pairs or requester_id in user_pairs:
                safe_reply(update, "❓ One of you is already in a chat\\. Try again later\\.", parse_mode="MarkdownV2")
                return ConversationHandler.END
            user_pairs[user_id] = requester_id
            user_pairs[requester_id] = user_id
        with chat_histories_lock:
            if has_premium_feature(user_id, "vaulted_chats"):
                chat_histories[user_id] = chat_histories.get(user_id, [])
            if has_premium_feature(requester_id, "vaulted_chats"):
                chat_histories[requester_id] = chat_histories.get(requester_id, [])
        safe_reply(update, "🔄 *Reconnected\\!* Start chatting\\! 🗣️", parse_mode="MarkdownV2")
        safe_bot_send_message(context.bot, requester_id, "🔄 *Reconnected\\!* Start chatting\\! 🗣️", parse_mode="MarkdownV2")
        try:
            context.bot.delete_message(chat_id=user_id, message_id=query.message.message_id)
        except TelegramError as e:
            logger.warning(f"Failed to delete rematch request message for {user_id}: {e}")
        with user_pairs_lock:
            context.bot_data["rematch_requests"] = context.bot_data.get("rematch_requests", {})
            if user_id in context.bot_data["rematch_requests"]:
                del context.bot_data["rematch_requests"][user_id]
        logger.info(f"User {user_id} accepted rematch with {requester_id}")
        return ConversationHandler.END

    # Handle rematch request decline
    if data == "rematch_decline":
        safe_reply(update, "❌ Rematch request declined\\.", parse_mode="MarkdownV2")
        with user_pairs_lock:
            context.bot_data["rematch_requests"] = context.bot_data.get("rematch_requests", {})
            if user_id in context.bot_data["rematch_requests"]:
                requester_id = context.bot_data["rematch_requests"][user_id]["requester_id"]
                safe_bot_send_message(context.bot, requester_id, "😔 Your rematch request was declined\\.", parse_mode="MarkdownV2")
                del context.bot_data["rematch_requests"][user_id]
        try:
            context.bot.delete_message(chat_id=user_id, message_id=query.message.message_id)
        except TelegramError as e:
            logger.warning(f"Failed to delete rematch request message for {user_id}: {e}")
        logger.info(f"User {user_id} declined rematch request")
        return ConversationHandler.END

    # Handle rematch request initiation
    if data.startswith("rematch_request_"):
        partner_id = int(data.split("_")[2])
        partner_data = get_user(partner_id)
        if not partner_data:
            safe_reply(update, "❌ This user is no longer available\\.", parse_mode="MarkdownV2")
            return ConversationHandler.END
        with user_pairs_lock:
            if user_id in user_pairs:
                safe_reply(update, "❓ You're already in a chat\\. Use /stop to end it first\\.", parse_mode="MarkdownV2")
                return ConversationHandler.END
            if partner_id in user_pairs:
                safe_reply(update, "❌ This user is currently in another chat\\.", parse_mode="MarkdownV2")
                return ConversationHandler.END
        keyboard = [
            [InlineKeyboardButton("✅ Accept", callback_data=f"rematch_accept_{user_id}"),
             InlineKeyboardButton("❌ Decline", callback_data="rematch_decline")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        user_profile = user.get("profile", {})
        request_message = (
            f"🔄 *Rematch Request* 🔄\n\n"
            f"A user wants to reconnect with you\\!\n"
            f"🧑 *Name*: {escape_markdown_v2(user_profile.get('name', 'Anonymous'))}\n"
            f"🎂 *Age*: {escape_markdown_v2(str(user_profile.get('age', 'Not set')))}\n"
            f"👤 *Gender*: {escape_markdown_v2(user_profile.get('gender', 'Not set'))}\n"
            f"📍 *Location*: {escape_markdown_v2(user_profile.get('location', 'Not set'))}\n\n"
            f"Would you like to chat again\\?"
        )
        try:
            message = context.bot.send_message(
                chat_id=partner_id,
                text=request_message,
                parse_mode="MarkdownV2",
                reply_markup=reply_markup
            )
            safe_reply(update, "📩 Rematch request sent\\. Waiting for their response\\...", parse_mode="MarkdownV2")
            context.bot_data["rematch_requests"] = context.bot_data.get("rematch_requests", {})
            context.bot_data["rematch_requests"][partner_id] = {
                "requester_id": user_id,
                "timestamp": int(time.time()),
                "message_id": message.message_id
            }
            logger.info(f"User {user_id} sent rematch request to {partner_id}")
        except TelegramError as e:
            safe_reply(update, "❌ Unable to reach the user\\. They may be offline\\.", parse_mode="MarkdownV2")
            logger.warning(f"Failed to send rematch request to {partner_id}: {e}")
        return ConversationHandler.END

    # Handle mood selection
    if data.startswith("mood_"):
        set_mood(update, context)
        return ConversationHandler.END

    # Handle settings menu
    if data == "help_menu":
        query.message.delete()
        context.user_data.pop("state", None)
        update_user(user_id, {"setup_state": None})
        help_text = (
            "👋 *Welcome to Talk2Anyone!* 👋\n\n"
            "Here’s how to get started:\n"
            "🔹 /start - Begin chatting with someone new\n"
            "🔹 /next - Switch to a new chat\n"
            "🔹 /stop - End current chat\n"
            "🔹 /settings - Customize your profile\n"
            "🔹 /premium - Unlock premium features\n"
            "🔹 /report - Report inappropriate behavior\n"
            "🔹 /delete_profile - Delete your data\n"
        )
        safe_reply(update, help_text, parse_mode="MarkdownV2")
        logger.info(f"User {user_id} returned to help menu")
        return ConversationHandler.END

    # Handle profile settings
    state_map = {
        "set_name": NAME,
        "set_age": AGE,
        "set_gender": GENDER,
        "set_location": LOCATION,
        "set_tags": TAGS
    }
    if data in state_map:
        context.user_data["state"] = state_map[data]
        try:
            if not update_user(user_id, {"setup_state": data}):
                logger.error(f"Failed to update setup_state for user {user_id}")
                safe_reply(update, "⚠️ Failed to update settings state\\. Please try again\\.", parse_mode="MarkdownV2")
                return ConversationHandler.END
        except PyMongoError as e:
            logger.error(f"MongoDB error updating setup_state for user {user_id}: {e}")
            safe_reply(update, "❌ Database error\\. Please try again\\.", parse_mode="MarkdownV2")
            return ConversationHandler.END
        prompts = {
            "set_name": "✍️ Please enter your name:",
            "set_age": "🎂 Please enter your age:",
            "set_gender": "👤 Please enter your gender:",
            "set_location": "📍 Please enter your location:",
            "set_tags": f"🏷️ Enter your tags (comma-separated, e.g., {','.join(ALLOWED_TAGS[:3])}):"
        }
        safe_reply(update, escape_markdown_v2(prompts[data]), parse_mode="MarkdownV2")
        logger.info(f"User {user_id} selected {data} in settings")
        return state_map[data]

    logger.warning(f"Unhandled callback data: {data} for user {user_id}")
    safe_reply(update, "⚠️ Invalid selection\\. Please try again\\.", parse_mode="MarkdownV2")
    return ConversationHandler.END

def message_handler(update: Update, context: CallbackContext) -> None:
    """Handle incoming messages"""
    user_id = update.effective_user.id
    user = get_user(user_id)
    if not user:
        safe_reply(update, "⚠️ User data not found\\. Please restart with /start\\.", parse_mode="MarkdownV2")
        return
    if is_banned(user_id):
        ban_type = user.get("ban_type", "temporary")
        ban_expiry = user.get("ban_expiry")
        ban_msg = (
            "🚫 You are permanently banned\\. Contact support to appeal\\."
            if ban_type == "permanent" else
            f"🚫 You are banned until {escape_markdown_v2(datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M'))}\\."
        )
        safe_reply(update, ban_msg, parse_mode="MarkdownV2")
        return
    state = context.user_data.get("state")
    if state in [NAME, AGE, GENDER, LOCATION, TAGS, CONSENT, VERIFICATION, SETTINGS]:
        handlers = {
            NAME: set_name,
            AGE: set_age,
            GENDER: set_gender,
            LOCATION: set_location,
            TAGS: set_tags,
            CONSENT: consent_handler,
            VERIFICATION: verify_emoji
        }
        if state in handlers:
            handlers[state](update, context)
        else:
            safe_reply(update, "⚠️ Invalid state\\. Please use /settings to continue\\.", parse_mode="MarkdownV2")
        return
    with user_pairs_lock:
        if user_id not in user_pairs:
            safe_reply(update, "❓ You're not in a chat\\. Use /start to begin\\.", parse_mode="MarkdownV2")
            return
        partner_id = user_pairs[user_id]
    message_text = update.message.text.strip()
    if not is_safe_message(message_text):
        safe_reply(update, "⚠️ Your message contains inappropriate content and was not sent\\.", parse_mode="MarkdownV2")
        issue_keyword_violation(user_id, message_text)
        logger.info(f"User {user_id} sent inappropriate message: {message_text}")
        return
    partner_data = get_user(partner_id)
    if not partner_data:
        safe_reply(update, "😔 Your partner is no longer available\\. Use /next to find a new one\\.", parse_mode="MarkdownV2")
        with user_pairs_lock:
            del user_pairs[user_id]
            if partner_id in user_pairs:
                del user_pairs[partner_id]
        return
    prefix = "🌟 " if has_premium_feature(user_id, "flare_messages") else ""
    formatted_message = f"{prefix}{escape_markdown_v2(message_text)}"
    try:
        safe_bot_send_message(context.bot, partner_id, formatted_message, parse_mode="MarkdownV2")
        with chat_histories_lock:
            if has_premium_feature(user_id, "vaulted_chats") and user_id in chat_histories:
                chat_histories[user_id].append({
                    "message": message_text,
                    "partner_id": partner_id,
                    "timestamp": int(time.time())
                })
            if has_premium_feature(partner_id, "vaulted_chats") and partner_id in chat_histories:
                chat_histories[partner_id].append({
                    "message": message_text,
                    "partner_id": user_id,
                    "timestamp": int(time.time())
                })
        user_activities[user_id] = user_activities.get(user_id, 0) + 1
        logger.info(f"Message sent from {user_id} to {partner_id}: {message_text}")
    except TelegramError as e:
        logger.error(f"Failed to send message from {user_id} to {partner_id}: {e}")
        safe_reply(update, "❌ Failed to send message\\. Your partner may be offline\\.", parse_mode="MarkdownV2")

def log_callback(update: Update, context: CallbackContext) -> None:
    """Log all callback queries for debugging"""
    query = update.callback_query
    if query:
        user_id = query.from_user.id
        data = query.data
        logger.info(f"Callback from user {user_id}: {data}")

def cleanup_rematch_requests(context: CallbackContext) -> None:
    """Clean up expired rematch requests"""
    current_time = int(time.time())
    bot_data = context.bot_data
    rematch_requests = bot_data.get("rematch_requests", {})
    expired = []
    for partner_id, request in rematch_requests.items():
        if current_time - request["timestamp"] > 3600:  # 1 hour expiry
            try:
                context.bot.delete_message(chat_id=partner_id, message_id=request["message_id"])
            except TelegramError as e:
                logger.warning(f"Failed to delete expired rematch request for {partner_id}: {e}")
            safe_bot_send_message(
                context.bot, request["requester_id"],
                "⏳ Your rematch request has expired\\.", parse_mode="MarkdownV2"
            )
            expired.append(partner_id)
    for partner_id in expired:
        del rematch_requests[partner_id]
    bot_data["rematch_requests"] = rematch_requests
    logger.info(f"Cleaned up {len(expired)} expired rematch requests")

def main() -> None:
    """Run the bot"""
    token = os.getenv("TOKEN")
    mongodb_uri = os.getenv("MONGODB_URI")
    if not token:
        logger.error("No TOKEN provided in environment variables")
        return
    if not mongodb_uri:
        logger.warning("No MONGODB_URI provided. MongoDB operations will fail.")

    try:
        init_mongodb()
        users_collection = get_db_collection("users")
        users_collection.create_index("user_id", unique=True)
        reports_collection = get_db_collection("reports")
        reports_collection.create_index([("reporter_id", 1), ("reported_id", 1)])
        violations_collection = get_db_collection("keyword_violations")
        violations_collection.create_index("user_id")
        logger.info("MongoDB indexes created successfully")
    except PyMongoError as e:
        logger.error(f"Failed to initialize MongoDB: {e}")
        return

    updater = Updater(token, use_context=True)
    dp = updater.dispatcher

    # Group 1: Setup conversation handler
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start), CommandHandler("settings", settings)],
        states={
            CONSENT: [MessageHandler(Filters.text & ~Filters.command, consent_handler)],
            NAME: [MessageHandler(Filters.text & ~Filters.command, set_name)],
            AGE: [MessageHandler(Filters.text & ~Filters.command, set_age)],
            GENDER: [MessageHandler(Filters.text & ~Filters.command, set_gender)],
            LOCATION: [MessageHandler(Filters.text & ~Filters.command, set_location)],
            VERIFICATION: [MessageHandler(Filters.text & ~Filters.command, verify_emoji)],
            TAGS: [MessageHandler(Filters.text & ~Filters.command, set_tags)],
            SETTINGS: [CallbackQueryHandler(button)]
        },
        fallbacks=[CommandHandler("start", start)]
    )
    dp.add_handler(conv_handler, group=1)

    # Group 2: Command handlers
    dp.add_handler(CommandHandler("next", next_chat), group=2)
    dp.add_handler(CommandHandler("stop", stop), group=2)
    dp.add_handler(CommandHandler("premium", premium), group=2)
    dp.add_handler(CommandHandler("report", report), group=2)
    dp.add_handler(CommandHandler("delete_profile", delete_profile), group=2)
    dp.add_handler(CommandHandler("shine", shine), group=2)
    dp.add_handler(CommandHandler("instant", instant), group=2)
    dp.add_handler(CommandHandler("mood", mood), group=2)
    dp.add_handler(CommandHandler("vault", vault), group=2)
    dp.add_handler(CommandHandler("history", history), group=2)
    dp.add_handler(CommandHandler("rematch", rematch), group=2)
    dp.add_handler(CommandHandler("flare", flare), group=2)

    # Group 3: Admin commands
    dp.add_handler(CommandHandler("admin", admin_access), group=3)
    dp.add_handler(CommandHandler("admin_userslist", admin_userslist), group=3)
    dp.add_handler(CommandHandler("admin_premiumuserslist", admin_premiumuserslist), group=3)
    dp.add_handler(CommandHandler("admin_info", admin_info), group=3)
    dp.add_handler(CommandHandler("admin_delete", admin_delete), group=3)
    dp.add_handler(CommandHandler("admin_premium", admin_premium), group=3)
    dp.add_handler(CommandHandler("admin_revoke_premium", admin_revoke_premium), group=3)
    dp.add_handler(CommandHandler("admin_ban", admin_ban), group=3)
    dp.add_handler(CommandHandler("admin_unban", admin_unban), group=3)
    dp.add_handler(CommandHandler("admin_violations", admin_violations), group=3)
    dp.add_handler(CommandHandler("admin_reports", admin_reports), group=3)
    dp.add_handler(CommandHandler("admin_clear_reports", admin_clear_reports), group=3)
    dp.add_handler(CommandHandler("admin_broadcast", admin_broadcast), group=3)
    dp.add_handler(CommandHandler("admin_stats", admin_stats), group=3)

    # Group 4: Payment handlers
    dp.add_handler(PreCheckoutQueryHandler(pre_checkout), group=4)
    dp.add_handler(MessageHandler(Filters.successful_payment, successful_payment), group=4)

    # Group 5: Message and callback handlers
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, message_handler), group=5)
    dp.add_handler(CallbackQueryHandler(button), group=5)

    # Group 10: Logging callbacks
    dp.add_handler(CallbackQueryHandler(log_callback), group=10)

    # Schedule cleanup job
    updater.job_queue.run_repeating(cleanup_rematch_requests, interval=600, first=10)

    updater.start_polling()
    logger.info("Bot started polling")
    updater.idle()

if __name__ == "__main__":
    main()
