from telegram import Update
from telegram.ext import (
    Updater,  # Use Updater instead of Application for v13.15
    CommandHandler,
    MessageHandler,
    Filters,
    CallbackContext,
)
import logging
import os

# Set up logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Store active users and their chat partners
waiting_users = []
user_pairs = {}
reported_chats = []

# Get the bot token from environment variables
TOKEN = os.getenv("TOKEN")

def start(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if user_id in user_pairs:
        update.message.reply_text("You're already in a chat. Use /next to switch or /stop to end.")
        return
    if user_id not in waiting_users:
        waiting_users.append(user_id)
        update.message.reply_text("Looking for a partner... 🔍")
    if len(waiting_users) >= 2:
        user1 = waiting_users.pop(0)
        user2 = waiting_users.pop(0)
        user_pairs[user1] = user2
        user_pairs[user2] = user1
        context.bot.send_message(user1, "Connected! Start chatting. 🗣️ Use /help for commands.")
        context.bot.send_message(user2, "Connected! Start chatting. 🗣️ Use /help for commands.")

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
    start(update, context)

def help_command(update: Update, context: CallbackContext) -> None:
    help_text = (
        "📋 *Chat Dude Commands*\n\n"
        "/start - Start a new anonymous chat\n"
        "/next - Find a new chat partner\n"
        "/stop - End the current chat\n"
        "/help - Show this help message\n"
        "/report - Report inappropriate behavior\n\n"
        "Stay respectful and enjoy chatting! 🗣️"
    )
    update.message.reply_text(help_text, parse_mode="Markdown")

def report(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        reported_chats.append({"reporter": user_id, "reported": partner_id})
        update.message.reply_text("Thank you for reporting. We’ll review the issue. Use /next to find a new partner.")
        logger.info(f"User {user_id} reported user {partner_id}")
    else:
        update.message.reply_text("You're not in a chat. Use /start to begin.")

def handle_message(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        context.bot.send_message(partner_id, update.message.text)
    else:
        update.message.reply_text("You're not connected. Use /start to find a partner.")

def error_handler(update: Update, context: CallbackContext) -> None:
    logger.error(f"Update {update} caused error {context.error}")
    if update:
        update.message.reply_text("An error occurred. Please try again or use /help for assistance.")

def main() -> None:
    if not TOKEN:
        logger.error("No TOKEN found in environment variables. Please set the TOKEN variable.")
        return

    # Use Updater for v13.15
    updater = Updater(TOKEN, use_context=True)

    # Get the dispatcher to register handlers
    dispatcher = updater.dispatcher

    # Add handlers
    dispatcher.add_handler(CommandHandler("start", start))
    dispatcher.add_handler(CommandHandler("stop", stop))
    dispatcher.add_handler(CommandHandler("next", next_chat))
    dispatcher.add_handler(CommandHandler("help", help_command))
    dispatcher.add_handler(CommandHandler("report", report))
    dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))
    dispatcher.add_error_handler(error_handler)

    logger.info("Starting Chat Dude bot...")
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
