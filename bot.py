from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters
from telegram import Update
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the bot token from environment variable
token = os.environ.get("TELEGRAM_BOT_TOKEN")

if not token:
    print("Error: TELEGRAM_BOT_TOKEN environment variable not set!")
    exit(1)

# Initialize the application
app = ApplicationBuilder().token(token).build()

# Command handler for /start
async def start(update: Update, context):
    await update.message.reply_text("Hello! I'm your crypto scanner bot. Use /scan to get started.")

# Message handler for regular messages
def echo(update: Update, context):
    update.message.reply_text(update.message.text)

# Register handlers
app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))

# Start the bot
print("Starting bot...")
app.run_polling()