import os
from pathlib import Path
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

TOKEN = "xxxxx"
REPORTS_DIR = Path("/root/airflow/reports")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет 👋\n"
        "Я бот для отчетов. Используй /reports чтобы получить список доступных отчетов."
    )

async def list_reports(update: Update, context: ContextTypes.DEFAULT_TYPE):
    weeks = sorted([f.name for f in REPORTS_DIR.iterdir() if f.is_dir()])

    if not weeks:
        await update.message.reply_text("❌ Нет доступных отчетов.")
        return

    keyboard = [[InlineKeyboardButton(week, callback_data=f"week|{week}")] for week in weeks]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text("Выберите неделю:", reply_markup=reply_markup)

async def week_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    _, week = query.data.split("|", 1)
    week_dir = REPORTS_DIR / week
    files = sorted(week_dir.glob("*.pdf"))

    if not files:
        await query.edit_message_text(f"❌ В папке {week} нет PDF отчетов")
        return
    
    context.user_data["files"] = {str(i): str(file) for i, file in enumerate(files)}

    keyboard = [
        [InlineKeyboardButton(file.name, callback_data=f"file|{i}")]
        for i, file in enumerate(files)
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(f"📂 Отчеты за {week}:", reply_markup=reply_markup)

async def file_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    _, file_id = query.data.split("|", 1)
    files = context.user_data.get("files", {})

    if file_id not in files:
        await query.edit_message_text("❌ Файл не найден")
        return

    file_path = Path(files[file_id])

    if not file_path.exists():
        await query.edit_message_text(f"❌ Файл {file_path.name} не найден")
        return

    await query.edit_message_text(f"Отправляю файл: {file_path.name}")
    await context.bot.send_document(chat_id=query.message.chat.id, document=open(file_path, "rb"))

def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("reports", list_reports))
    app.add_handler(CallbackQueryHandler(week_callback, pattern=r"^week\|"))
    app.add_handler(CallbackQueryHandler(file_callback, pattern=r"^file\|"))

    print("🤖 Bot started...")
    app.run_polling()


if __name__ == "__main__":
    main()
