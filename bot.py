# bot.py
import asyncio
import os
import traceback
from datetime import datetime, time, timedelta, date
from typing import Optional

import pytz
from telegram import Update, ReplyKeyboardMarkup, BotCommand
from telegram.helpers import escape_markdown
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
    ConversationHandler
)

import config
import excel_reporter
from services import HolidayService

config.setup_logging()
logger = config.get_logger(__name__)

BTN_GET_HOLIDAYS = "Узнать праздники на день 📅"
BTN_CREATE_REPORT = "Сгенерировать Excel-отчет 📊"

GET_START_DATE, GET_END_DATE, GET_SPECIFIC_DATE = range(3)


# --- ИЗМЕНЕНО: Обновляем вспомогательную функцию для вывода регионов ---
async def _create_holidays_message(target_date: date) -> Optional[str]:
    """
    Формирует текстовое сообщение о праздниках на указанную дату, включая регионы.
    """
    try:
        target_date_str = target_date.strftime('%Y-%m-%d')
        target_date_formatted = target_date.strftime('%d.%m.%Y')

        holiday_service = HolidayService()
        # Получаем данные в новой структуре: {'RU': {'Праздник': ['Регион1', 'Регион2']}}
        holidays_by_country = holiday_service.get_holidays_for_date(target_date_str)

        escaped_date = escape_markdown(target_date_formatted, version=2)

        if not holidays_by_country:
            return f"🗓️ На {escaped_date} праздников не найдено\\."

        message_parts = [f"🎉 *Праздники на {escaped_date}* 🎉\n"]
        for country_code in sorted(holidays_by_country.keys()):
            escaped_country_name = escape_markdown(country_code.upper(), version=2)
            message_parts.append(f"\n*{escaped_country_name}*")

            # Получаем словарь {название_праздника: [регионы]}
            holiday_details = holidays_by_country[country_code]

            # Итерируемся по праздникам и их регионам
            for holiday_name, regions in holiday_details.items():
                escaped_holiday_name = escape_markdown(holiday_name, version=2)

                # Если у праздника есть регионы, добавляем их через тире
                if regions:
                    escaped_regions = escape_markdown(", ".join(regions), version=2)
                    message_parts.append(f"  \\- {escaped_holiday_name} \\- _{escaped_regions}_")
                else:
                    # Если регионов нет, выводим только название
                    message_parts.append(f"  \\- {escaped_holiday_name}")

        return "\n".join(message_parts)
    except Exception as e:
        logger.error(f"Ошибка при создании сообщения о праздниках для даты {target_date}: {e}", exc_info=True)
        return None


# --- Остальные функции bot.py остаются без изменений ---

async def send_daily_holidays_notification(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    log_ctx = {'job_name': job.name if job else 'manual_run'}
    logger.info("Запуск задачи по отправке уведомлений о праздниках.", extra={'context': log_ctx})

    tz = pytz.timezone(config.TZ_INFO)
    today = (datetime.now(tz)).date()
    message_text = await _create_holidays_message(today)

    if message_text:
        await context.bot.send_message(
            chat_id=config.TELEGRAM_CHANNEL_ID,
            text=message_text,
            parse_mode='MarkdownV2'
        )
        logger.info(f"Сообщение успешно отправлено в канал {config.TELEGRAM_CHANNEL_ID}.", extra={'context': log_ctx})
    else:
        logger.error("Не удалось сформировать сообщение о праздниках.", extra={'context': log_ctx})


async def start_holiday_check_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "Пожалуйста, введите дату для проверки в формате `ГГГГ-ММ-ДД`.\n\n"
        "Чтобы отменить, введите /cancel.",
        parse_mode='Markdown'
    )
    return GET_SPECIFIC_DATE


async def handle_specific_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_input = update.message.text
    try:
        target_date = datetime.strptime(user_input, '%Y-%m-%d').date()
        logger.info(f"Пользователь {update.effective_user.id} запросил праздники на дату: {user_input}")
        await update.message.reply_text("🔍 Ищу информацию, пожалуйста, подождите...")
        message_text = await _create_holidays_message(target_date)
        if message_text:
            await update.message.reply_text(message_text, parse_mode='MarkdownV2')
        else:
            await update.message.reply_text("❌ Произошла ошибка при получении данных. Попробуйте позже.")
        return ConversationHandler.END
    except ValueError:
        await update.message.reply_text("❌ Неверный формат. Введите дату `ГГГГ-ММ-ДД`.")
        return GET_SPECIFIC_DATE


async def post_init(application: Application):
    await application.bot.set_my_commands([
        BotCommand("start", "🚀 Перезапустить бота"),
        BotCommand("cancel", "❌ Отменить текущую операцию"),
    ])
    logger.info("Команды бота успешно установлены.")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    logger.info(f"Пользователь {user.username} (ID: {user.id}) запустил бота.")
    keyboard = [[BTN_GET_HOLIDAYS], [BTN_CREATE_REPORT]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)
    await update.message.reply_html(
        f"👋 Привет, {user.mention_html()}!\n\n"
        "Я помогу тебе узнать о праздниках или создать Excel-отчет.",
        reply_markup=reply_markup,
    )
    return ConversationHandler.END


async def start_report_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "Вы начали создание отчета.\n"
        "Пожалуйста, введите **дату начала** периода в формате `ГГГГ-ММ-ДД`.\n\n"
        "Чтобы отменить, введите /cancel.",
        parse_mode='Markdown'
    )
    return GET_START_DATE


async def handle_start_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_input = update.message.text
    try:
        datetime.strptime(user_input, '%Y-%m-%d')
        context.user_data['start_date'] = user_input
        await update.message.reply_text(
            f"Отлично! Дата начала: `{user_input}`.\n"
            "Теперь введите **дату окончания** в том же формате (`ГГГГ-ММ-ДД`).",
            parse_mode='Markdown'
        )
        return GET_END_DATE
    except ValueError:
        await update.message.reply_text("❌ Неверный формат. Введите дату `ГГГГ-ММ-ДД`.")
        return GET_START_DATE


async def handle_end_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_input = update.message.text
    start_date_str = context.user_data.get('start_date')
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(user_input, '%Y-%m-%d')
        if end_date < start_date:
            await update.message.reply_text("❌ Дата окончания не может быть раньше даты начала.")
            return GET_END_DATE
        await update.message.reply_text("⏳ Генерирую Excel-файл...")
        report_path = await asyncio.to_thread(
            excel_reporter.generate_holidays_report, start_date=start_date_str, end_date=user_input
        )
        with open(report_path, 'rb') as report_file:
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=report_file,
                filename=os.path.basename(report_path),
                caption="✅ Ваш Excel-отчет готов!"
            )
        if report_path and os.path.exists(report_path):
            os.remove(report_path)
        context.user_data.clear()
        return ConversationHandler.END
    except ValueError:
        await update.message.reply_text("❌ Неверный формат. Введите дату `ГГГГ-ММ-ДД`.")
        return GET_END_DATE


async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    logger.info(f"Пользователь {user.id} отменил операцию.")
    context.user_data.clear()
    await update.message.reply_text("Операция отменена. Чем еще могу помочь?")
    return ConversationHandler.END


def main():
    logger.info("Запуск Telegram-бота...")
    if not config.TELEGRAM_BOT_TOKEN:
        logger.critical("Токен TELEGRAM_BOT_TOKEN не найден! Бот не может быть запущен.")
        return

    application = ApplicationBuilder().token(config.TELEGRAM_BOT_TOKEN).post_init(post_init).build()

    job_queue = application.job_queue
    if config.TELEGRAM_CHANNEL_ID and config.DAILY_NOTIFICATION_TIME:
        try:
            h, m = map(int, config.DAILY_NOTIFICATION_TIME.split(':'))
            notification_time = time(h, m, tzinfo=pytz.timezone(config.TZ_INFO))
            job_queue.run_daily(send_daily_holidays_notification, time=notification_time,
                                name="daily_holiday_notification")
            logger.info(
                f"Запланирована ежедневная отправка уведомлений в {config.DAILY_NOTIFICATION_TIME} ({config.TZ_INFO}).")
        except (ValueError, TypeError) as e:
            logger.error(f"Неверный формат времени DAILY_NOTIFICATION_TIME: {e}")

    holiday_check_conv_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.TEXT & filters.Regex(f'^{BTN_GET_HOLIDAYS}$'), start_holiday_check_conversation)],
        states={GET_SPECIFIC_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_specific_date)]},
        fallbacks=[CommandHandler('cancel', cancel_conversation), CommandHandler('start', start)],
    )

    report_conv_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.TEXT & filters.Regex(f'^{BTN_CREATE_REPORT}$'), start_report_conversation)],
        states={
            GET_START_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_start_date)],
            GET_END_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_end_date)],
        },
        fallbacks=[CommandHandler('cancel', cancel_conversation), CommandHandler('start', start)],
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(holiday_check_conv_handler)
    application.add_handler(report_conv_handler)

    application.run_polling()
    logger.info("Бот остановлен.")


if __name__ == "__main__":
    main()
