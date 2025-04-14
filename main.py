from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    CallbackContext,
    ConversationHandler,
    CallbackQueryHandler,
)
import logging
import os
import time
from datetime import datetime

# Set up logging for Heroku (output to stdout)
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO if os.getenv("DEBUG_MODE") != "true" else logging.DEBUG,
    handlers=[logging.StreamHandler()]  # Log to stdout for Heroku
)
logger = logging.getLogger(__name__)

# Security configurations
BANNED_WORDS = {
    "spam", "hate", "abuse", "nsfw", "inappropriate", "offensive",
    "harass", "bully", "threat", "sex", "porn", "nude", "violence"
}
COMMAND_COOLDOWN = 10
MAX_MESSAGES_PER_MINUTE = 10
REPORT_THRESHOLD = 3
TEMP_BAN_DURATION = 24 * 3600

# In-memory storage for Heroku (ephemeral, non-persistent)
banned_users = {}  # {user_id: {"type": "temporary/permanent", "expiry": timestamp}}
premium_users = {}  # {user_id: expiry_timestamp}
consent_log = {}   # {user_id: consent_timestamp}
waiting_users = []
user_pairs = {}
previous_partners = {}
reported_chats = []
user_profiles = {}
user_consents = set()
command_timestamps = {}
message_timestamps = {}

# Disable AI moderation for Heroku free dynos (uncomment to enable with Standard dyno)
# from transformers import pipeline
# try:
#     toxicity_classifier = pipeline("text-classification", model="unitary/toxic-bert")
# except Exception as e:
#     logger.warning(f"Could not load toxicity classifier: {e}")
#     toxicity_classifier = None
toxicity_classifier = None

# Conversation states
GENDER, AGE, TAGS, LOCATION, CONSENT = range(5)

# Get bot token
TOKEN = os.getenv("TOKEN")
if not TOKEN:
    logger.error("No TOKEN found in environment variables. Exiting.")
    exit(1)

# Helper functions for security
def is_banned(user_id: int) -> bool:
    """Check if a user is banned."""
    if user_id in banned_users:
        ban = banned_users[user_id]
        if ban["type"] == "permanent" or (ban["type"] == "temporary" and ban["expiry"] > time.time()):
            return True
        if ban["type"] == "temporary" and ban["expiry"] <= time.time():
            del banned_users[user_id]
    return False

def is_premium(user_id: int) -> bool:
    """Check if a user has an active premium subscription."""
    if user_id in premium_users and premium_users[user_id] > time.time():
        return True
    return False

def log_consent(user_id: int):
    """Log user consent with timestamp."""
    consent_log[user_id] = time.time()
    logger.info(f"User {user_id} consent logged at {datetime.fromtimestamp(consent_log[user_id])}.")

def check_rate_limit(user_id: int, cooldown: int = COMMAND_COOLDOWN) -> bool:
    """Check command rate limit."""
    current_time = time.time()
    if user_id in command_timestamps and current_time - command_timestamps[user_id] < cooldown:
        return False
    command_timestamps[user_id] = current_time
    return True

def check_message_rate_limit(user_id: int) -> bool:
    """Check message rate limit."""
    current_time = time.time()
    if user_id not in message_timestamps:
        message_timestamps[user_id] = []
    message_timestamps[user_id] = [t for t in message_timestamps[user_id] if current_time - t < 60]
    if len(message_timestamps[user_id]) >= MAX_MESSAGES_PER_MINUTE:
        return False
    message_timestamps[user_id].append(current_time)
    return True

# Bot commands
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
    if user_id not in user_consents:
        keyboard = [
            [InlineKeyboardButton("I Agree", callback_data="consent_agree")],
            [InlineKeyboardButton("I Disagree", callback_data="consent_disagree")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        update.message.reply_text(
            "Welcome to Talk2Anyone! 🌟\n\n"
            "I connect you with random people for anonymous chats. Rules:\n"
            "- No harassment, spam, or inappropriate content.\n"
            "- Respect other users at all times.\n"
            "- Use /report to report issues.\n"
            "- Violations may result in a ban.\n\n"
            "Privacy: We store only your user ID, profile settings, and consent timestamp (ephemeral on Heroku). Use /deleteprofile to remove your data.\n\n"
            "Do you agree to these rules?",
            reply_markup=reply_markup
        )
        return CONSENT
    if user_id not in waiting_users:
        if is_premium(user_id):
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
        user_consents.add(user_id)
        log_consent(user_id)
        query.message.reply_text("Thank you for agreeing! Let’s get started.")
        if user_id not in waiting_users:
            if is_premium(user_id):
                waiting_users.insert(0, user_id)
            else:
                waiting_users.append(user_id)
            query.message.reply_text("Looking for a partner... 🔍")
        match_users(context)
    else:
        query.message.reply_text("You must agree to the rules to use this bot. Use /start to try again.")
        logger.info(f"User {user_id} declined rules.")
    return ConversationHandler.END

def match_users(context: CallbackContext) -> None:
    if len(waiting_users) < 2:
        return
    for i in range(len(waiting_users)):
        user1 = waiting_users[i]
        for j in range(i + 1, len(waiting_users)):
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
                return

def can_match(user1: int, user2: int) -> bool:
    profile1 = user_profiles.get(user1, {})
    profile2 = user_profiles.get(user2, {})
    if not profile1 or not profile2:
        return True
    tags1 = profile1.get("tags", [])
    tags2 = profile2.get("tags", [])
    if tags1 and tags2:
        if not any(tag in tags2 for tag in tags1):
            return False
    age1 = profile1.get("age")
    age2 = profile2.get("age")
    if age1 and age2:
        if abs(age1 - age2) > 10:
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
        del user_pairs[partner_id]
        context.bot.send_message(partner_id, "Your partner left the chat. Use /start to find a new one.")
        update.message.reply_text("Chat ended. Use /start to begin a new chat.")
        logger.info(f"User {user_id} stopped chat with {partner_id}.")
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
    if user_id in user_consents:
        if user_id not in waiting_users:
            if is_premium(user_id):
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
        "/start - Start a new anonymous chat\n"
        "/next - Find a new chat partner\n"
        "/stop - End the current chat\n"
        "/help - Show this help message\n"
        "/report - Report inappropriate behavior\n"
        "/settings - Customize your profile and matching filters\n"
        "/rematch - Reconnect with your previous partner (Premium)\n"
        "/deleteprofile - Delete your profile and data\n\n"
        "Stay respectful and enjoy chatting! 🗣️"
    )
    update.message.reply_text(help_text, parse_mode="Markdown")

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
        reported_chats.append({
            "reporter": user_id,
            "reported": partner_id,
            "timestamp": time.time(),
            "reporter_profile": user_profiles.get(user_id, {}),
            "reported_profile": user_profiles.get(partner_id, {})
        })
        report_count = sum(1 for r in reported_chats if r["reported"] == partner_id)
        if report_count >= REPORT_THRESHOLD:
            banned_users[partner_id] = {"type": "temporary", "expiry": time.time() + TEMP_BAN_DURATION}
            context.bot.send_message(partner_id, "You’ve been temporarily banned due to multiple reports.")
            logger.warning(f"User {partner_id} banned temporarily due to {report_count} reports.")
            stop(update, context)
        update.message.reply_text("Thank you for reporting. We’ll review the issue. Use /next to find a new partner.")
        logger.info(f"User {user_id} reported user {partner_id}. Total reports: {report_count}.")
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
        message_text = update.message.text.lower()
        if any(word in message_text for word in BANNED_WORDS):
            update.message.reply_text("Inappropriate content detected. Please keep the chat respectful.")
            logger.info(f"User {user_id} sent message with banned word: {message_text}")
            return
        # AI moderation (uncomment if enabled)
        # if toxicity_classifier:
        #     try:
        #         result = toxicity_classifier(message_text)
        #         if result[0]["label"] == "toxic" and result[0]["score"] > 0.8:
        #             update.message.reply_text("Inappropriate content detected. Please keep the chat respectful.")
        #             logger.info(f"User {user_id} sent toxic message: {message_text} (score: {result[0]['score']})")
        #             return
        #     except Exception as e:
        #         logger.error(f"Toxicity check failed: {e}")
        context.bot.send_message(partner_id, update.message.text)
        logger.info(f"Message from {user_id} to {partner_id}: {update.message.text}")
    else:
        update.message.reply_text("You're not connected. Use /start to find a partner.")

def settings(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return ConversationHandler.END
    if not check_rate_limit(user_id):
        update.message.reply_text(f"Please wait {COMMAND_COOLDOWN} seconds before trying again.")
        return ConversationHandler.END
    if user_id not in user_profiles:
        user_profiles[user_id] = {}
    keyboard = [
        [InlineKeyboardButton("Gender", callback_data="gender")],
        [InlineKeyboardButton("Age", callback_data="age")],
        [InlineKeyboardButton("Tags", callback_data="tags")],
        [InlineKeyboardButton("Location", callback_data="location")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    update.message.reply_text(
        "Customize your profile and add matching filters "
        "(note: filters may increase matching time)",
        reply_markup=reply_markup
    )
    return GENDER

def button(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
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
        query.message.reply_text("Please enter your age (e.g., 25):")
        return AGE
    elif choice == "tags":
        query.message.reply_text("Enter tags (e.g., music, gaming, movies) separated by commas:")
        return TAGS
    elif choice == "location":
        query.message.reply_text("Enter your location (e.g., New York):")
        return LOCATION

def set_gender(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    choice = query.data
    if choice.startswith("gender_"):
        gender = choice.split("_")[1]
        if gender != "skip":
            user_profiles[user_id]["gender"] = gender
        else:
            user_profiles[user_id].pop("gender", None)
        query.message.reply_text(f"Gender set to: {gender if gender != 'skip' else 'Not specified'}")
    return settings(update, context)

def set_age(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
    try:
        age = int(update.message.text)
        if 13 <= age <= 120:
            user_profiles[user_id]["age"] = age
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
    tags = [tag.strip().lower() for tag in update.message.text.split(",") if tag.strip()]
    user_profiles[user_id]["tags"] = tags
    update.message.reply_text(f"Tags set to: {', '.join(tags) if tags else 'None'}")
    return settings(update, context)

def set_location(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
    location = update.message.text.strip()
    user_profiles[user_id]["location"] = location
    update.message.reply_text(f"Location set to: {location}")
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
        update.message.reply_text(
            "Re-matching is a PREMIUM feature. Subscribe to use it.\n"
            "100 ⭐ / $1.99 Week\n"
            "350 ⭐ / $3.99 Month\n"
            "1000 ⭐ / $19.99 Year"
        )
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

def delete_profile(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if not check_rate_limit(user_id):
        update.message.reply_text(f"Please wait {COMMAND_COOLDOWN} seconds before trying again.")
        return
    if user_id in user_profiles:
        del user_profiles[user_id]
    if user_id in user_consents:
        user_consents.remove(user_id)
    if user_id in consent_log:
        del consent_log[user_id]
    update.message.reply_text("Your profile and data have been deleted where possible.")
    logger.info(f"User {user_id} deleted their profile.")

def error_handler(update: Update, context: CallbackContext) -> None:
    logger.error(f"Update {update} caused error {context.error}")
    if update and update.message:
        update.message.reply_text("An error occurred. Please try again or use /help for assistance.")

def main() -> None:
    updater = Updater(TOKEN, use_context=True)
    dispatcher = updater.dispatcher
    start_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            CONSENT: [CallbackQueryHandler(consent_handler, pattern="^consent_")],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    settings_handler = ConversationHandler(
        entry_points=[CommandHandler("settings", settings)],
        states={
            GENDER: [CallbackQueryHandler(button), CallbackQueryHandler(set_gender, pattern="^gender_")],
            AGE: [MessageHandler(Filters.text & ~Filters.command, set_age)],
            TAGS: [MessageHandler(Filters.text & ~Filters.command, set_tags)],
            LOCATION: [MessageHandler(Filters.text & ~Filters.command, set_location)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    dispatcher.add_handler(start_handler)
    dispatcher.add_handler(settings_handler)
    dispatcher.add_handler(CommandHandler("stop", stop))
    dispatcher.add_handler(CommandHandler("next", next_chat))
    dispatcher.add_handler(CommandHandler("help", help_command))
    dispatcher.add_handler(CommandHandler("report", report))
    dispatcher.add_handler(CommandHandler("rematch", rematch))
    dispatcher.add_handler(CommandHandler("deleteprofile", delete_profile))
    dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))
    dispatcher.add_error_handler(error_handler)
    logger.info("Starting Talk2Anyone bot...")
    updater.start_polling(drop_pending_updates=True)
    updater.idle()

if __name__ == "__main__":
    main()
