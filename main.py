from telegram import Update
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    CallbackContext,
    ConversationHandler,
    CallbackQueryHandler,
)
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import logging
import os

# Set up logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Store active users, their chat partners, and user profiles
waiting_users = []
user_pairs = {}
previous_partners = {}
reported_chats = []
user_profiles = {}
premium_users = set()  # Simulate premium users (add user IDs manually for testing)
user_consents = set()  # Track users who have agreed to the rules

# Expanded list of banned words for moderation
BANNED_WORDS = {
    "spam", "hate", "abuse", "nsfw", "inappropriate", "offensive", 
    "harass", "bully", "threat", "sex", "porn", "nude", "violence"
}

# Conversation states for settings and consent
GENDER, AGE, TAGS, LOCATION, CONSENT = range(5)

# Get the bot token from environment variables
TOKEN = os.getenv("TOKEN")

def start(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
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
            "I connect you with random people for anonymous chats. To keep our community safe, please agree to our rules:\n"
            "- No harassment, spam, or inappropriate content.\n"
            "- Respect other users at all times.\n"
            "- Use /report to report any issues.\n"
            "- Violations may result in a ban.\n\n"
            "Do you agree to these rules?",
            reply_markup=reply_markup
        )
        return CONSENT
    if user_id not in waiting_users:
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
        query.message.reply_text("Thank you for agreeing! Let’s get started.")
        if user_id not in waiting_users:
            waiting_users.append(user_id)
            query.message.reply_text("Looking for a partner... 🔍")
        match_users(context)
    else:
        query.message.reply_text("You must agree to the rules to use this bot. If you change your mind, use /start again.")
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
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        del user_pairs[user_id]
        del user_pairs[partner_id]
        context.bot.send_message(partner_id, "Your partner left the chat. Use /start to find a new one.")
        update.message.reply_text("Chat ended. Use /start to begin a new chat.")
    else:
        update.message.reply_text("You're not in a chat. Use /start to find a partner.")

def next_chat(update: Update, context: CallbackContext) -> None:
    stop(update, context)
    user_id = update.message.from_user.id
    if user_id in user_consents:
        if user_id not in waiting_users:
            waiting_users.append(user_id)
            update.message.reply_text("Looking for a partner... 🔍")
        match_users(context)
    else:
        update.message.reply_text("Please agree to the rules first by using /start.")

def help_command(update: Update, context: CallbackContext) -> None:
    help_text = (
        "📋 *Talk2Anyone Commands*\n\n"
        "/start - Start a new anonymous chat\n"
        "/next - Find a new chat partner\n"
        "/stop - End the current chat\n"
        "/help - Show this help message\n"
        "/report - Report inappropriate behavior\n"
        "/settings - Customize your profile and matching filters\n"
        "/rematch - Reconnect with your previous partner (Premium feature)\n\n"
        "Stay respectful and enjoy chatting! 🗣️"
    )
    update.message.reply_text(help_text, parse_mode="Markdown")

def report(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        reported_chats.append({
            "reporter": user_id,
            "reported": partner_id,
            "reporter_profile": user_profiles.get(user_id, {}),
            "reported_profile": user_profiles.get(partner_id, {})
        })
        update.message.reply_text("Thank you for reporting. We’ll review the issue. Use /next to find a new partner.")
        logger.info(f"User {user_id} reported user {partner_id}. Reporter profile: {user_profiles.get(user_id, {})}. Reported profile: {user_profiles.get(partner_id, {})}")
    else:
        update.message.reply_text("You're not in a chat. Use /start to begin.")

def handle_message(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        message_text = update.message.text.lower()
        if any(word in message_text for word in BANNED_WORDS):
            update.message.reply_text("Inappropriate content detected. Please keep the chat respectful. Use /report if needed.")
            return
        context.bot.send_message(partner_id, update.message.text)
    else:
        update.message.reply_text("You're not connected. Use /start to find a partner.")

def settings(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
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
        "Here you can customize your profile and add matching filters "
        "(note: adding matching filters WILL increase the matching time)",
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
    update.message.reply_text("Settings cancelled.")
    return ConversationHandler.END

def rematch(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if user_id not in premium_users:
        update.message.reply_text(
            "Re-matching with previous partners is a PREMIUM feature. "
            "Please subscribe to use this feature.\n\n"
            "100 ⭐ / $1.99 Week\n"
            "350 ⭐ / $3.99 Month\n"
            "1000 ⭐ / $19.99 Year"
        )
        return

    if user_id in user_pairs:
        update.message.reply_text("You're already in a chat. Use /stop to end the current chat first.")
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

def error_handler(update: Update, context: CallbackContext) -> None:
    logger.error(f"Update {update} caused error {context.error}")
    if update and update.message:
        update.message.reply_text("An error occurred. Please try again or use /help for assistance.")

def main() -> None:
    if not TOKEN:
        logger.error("No TOKEN found in environment variables. Please set the TOKEN variable.")
        return

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
    dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))
    dispatcher.add_error_handler(error_handler)

    logger.info("Starting Talk2Anyone bot...")
    updater.start_polling(drop_pending_updates=True)
    updater.idle()

if __name__ == "__main__":
    main()
