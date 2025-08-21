import asyncio
import os
import traceback
from datetime import datetime, time, timedelta, date
from typing import Optional
import calendar

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
    ConversationHandler,
    PicklePersistence 
)

import config
import excel_reporter
import email_sender
from services import HolidayService

config.setup_logging()
logger = config.get_logger(__name__)

BTN_GET_HOLIDAYS = "Узнать праздники на день 📅"
BTN_CREATE_REPORT = "Сгенерировать Excel-отчет 📊"

GET_START_DATE, GET_END_DATE, GET_SPECIFIC_DATE = range(3)


# --- НОВЫЕ ФУНКЦИИ ДЛЯ ЕЖЕМЕСЯЧНОЙ ЗАДАЧИ ---

def get_next_date_for_job():
    """Рассчитывает даты для следующего месяца. Аналог функции из main.py"""
    today = date.today()
    year = today.year
    if today.month == 12:
        next_month = 1
        year += 1
    else:
        next_month = today.month + 1

    last_day_of_month = calendar.monthrange(year, next_month)[1]
    first_day = date(year, next_month, 1).isoformat()
    last_day = date(year, next_month, last_day_of_month).isoformat()
    return str(year), str(next_month).zfill(2), first_day, last_day


async def run_monthly_data_collection(context: ContextTypes.DEFAULT_TYPE):
    """
    Основная логика сбора данных за следующий месяц и отправки отчетов.
    Вызывается планировщиком.
    """
    job_name = context.job.name if context.job else 'manual_run'
    log_ctx = {'job_name': job_name}
    logger.info("Запуск ежемесячной задачи по сбору данных о праздниках...", extra={'context': log_ctx})

    await context.bot.send_message(
        chat_id=config.TELEGRAM_CHANNEL_ID,
        text="🚀 Начинаю ежемесячный сбор данных о праздниках на следующий месяц..."
    )

    try:
        countries_for_holidays = config.COUNTRIES
        if not countries_for_holidays:
            logger.warning("Список стран для сбора праздников пуст. Обработка пропускается.",
                           extra={'context': log_ctx})
            await context.bot.send_message(
                chat_id=config.TELEGRAM_CHANNEL_ID,
                text="⚠️ Список стран для обработки пуст. Ежемесячный сбор данных пропущен."
            )
            return

        year, next_month_str, first_day, last_day = get_next_date_for_job()
        period_str = f"{next_month_str}/{year}"
        logger.info(f"Целевой период для сбора: {period_str}", extra={'context': log_ctx})

        # Инициализируем сервис
        holiday_service = HolidayService()

        # Запускаем обработку для каждой страны
        for country_code in countries_for_holidays:
            try:
                holiday_service.process_holidays_for_period(
                    country_code=country_code,
                    year=year,
                    month=next_month_str,
                    first_day=first_day,
                    last_day=last_day
                )
            except Exception as e:
                logger.critical(f"Критическая ошибка при обработке страны {country_code}: {e}", exc_info=True)
                # Отправим сообщение об ошибке, но продолжим с другими странами
                await context.bot.send_message(
                    chat_id=config.TELEGRAM_CHANNEL_ID,
                    text=f"❌ Критическая ошибка при обработке страны {country_code}: {e}"
                )

        # Экранируем все динамические части для безопасности
        escaped_period = escape_markdown(period_str, version=2)
        escaped_countries = escape_markdown(', '.join(countries_for_holidays), version=2)
        escaped_tokens = escape_markdown(str(holiday_service.grand_total_tokens), version=2)

        price_str = f"{holiday_service.grand_total_price:.4f}"
        escaped_price = escape_markdown(price_str, version=2)

        summary_message = (
            f"✅ *Ежемесячный сбор данных успешно завершен* ✨\n\n"
            f"*Обработанный период:* `{escaped_period}`\n"
            f"*Страны:* `{escaped_countries}`\n\n"
            f"📊 *Итоги по экономике:*\n"
            f"  • Всего потрачено токенов: `{escaped_tokens}`\n"
            f"  • Итоговая стоимость: `{escaped_price}$`\n\n"
            f"⏳ Начинаю генерацию Excel отчета\\.\\.\\."
        )
        await context.bot.send_message(
            chat_id=config.TELEGRAM_CHANNEL_ID,
            text=summary_message,
            parse_mode='MarkdownV2'
        )
        # Генерируем и отправляем Excel-отчет
        logger.info("Генерация Excel-отчета...", extra={'context': log_ctx})
        report_path = await asyncio.to_thread(
            excel_reporter.generate_holidays_report, start_date=first_day, end_date=last_day
        )
        with open(report_path, 'rb') as report_file:
            await context.bot.send_document(
                chat_id=config.TELEGRAM_CHANNEL_ID,
                document=report_file,
                filename=os.path.basename(report_path),
                caption=f"📊 Excel-отчет по праздникам на {period_str} готов!"
            )
        if report_path and os.path.exists(report_path):
            os.remove(report_path)
            logger.info(f"Временный файл отчета {report_path} удален.", extra={'context': log_ctx})

        # --- НАЧАЛО НОВОГО БЛОКА: ОТПРАВКА EMAIL-УВЕДОМЛЕНИЙ ---
        if config.EMAIL_NOTIFICATIONS_ENABLED:
            logger.info("Начинаю отправку email-уведомлений...")
            await context.bot.send_message(
                chat_id=config.TELEGRAM_CHANNEL_ID,
                text="📧 Рассылаю email-уведомления подписчикам..."
            )
            try:
                # Получаем имя месяца для письма
                month_names = ["", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                               "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
                month_name = month_names[int(next_month_str)]

                # Запускаем отправку в отдельном потоке, чтобы не блокировать бота
                email_result = await asyncio.to_thread(
                    email_sender.send_holiday_email_to_all,
                    year=int(year),
                    month_name=month_name,
                    start_date=first_day,
                    end_date=last_day
                )

                if email_result.get('success'):
                    success_msg = (
                        f"✅ Email\\-рассылка успешно завершена\\.\n"
                        f"Отправлено писем: `{email_result.get('sent_count', 'N/A')}` "
                        f"из `{email_result.get('total_recipients', 'N/A')}`\\."
                    )
                    await context.bot.send_message(
                        chat_id=config.TELEGRAM_CHANNEL_ID,
                        text=success_msg,
                        parse_mode='MarkdownV2'
                    )
                else:
                    error_msg = f"⚠️ Произошла ошибка при отправке email-уведомлений: {email_result.get('error', 'Неизвестная ошибка')}"
                    logger.error(error_msg, extra={'context': log_ctx})
                    await context.bot.send_message(
                        chat_id=config.TELEGRAM_CHANNEL_ID,
                        text=escape_markdown(error_msg, version=2),
                        parse_mode='MarkdownV2'
                    )
            except Exception as e:
                logger.critical("Критическая ошибка в модуле отправки email.", exc_info=True)
                error_message = (
                    f"❌ *Критическая ошибка при отправке email* ❌\n\n"
                    f"`{escape_markdown(str(e), version=2)}`"
                )
                await context.bot.send_message(
                    chat_id=config.TELEGRAM_CHANNEL_ID,
                    text=error_message,
                    parse_mode='MarkdownV2'
                )
        else:
            logger.info("Email-уведомления отключены в конфигурации.")
        # --- КОНЕЦ НОВОГО БЛОКА ---


    except Exception as e:
        logger.critical("Произошла критическая ошибка во время выполнения ежемесячной задачи.", exc_info=True)
        error_message = (
            f"❌ *Критическая ошибка при выполнении ежемесячного сбора данных* ❌\n\n"
            f"Произошла непредвиденная ошибка\\. Проверьте логи для детальной информации\\.\n\n"
            f"*Текст ошибки:*\n`{escape_markdown(str(e), version=2)}`\n\n"
            f"*Traceback:*\n```\n{escape_markdown(traceback.format_exc(limit=1), version=2)}\n```"
        )
        await context.bot.send_message(
            chat_id=config.TELEGRAM_CHANNEL_ID,
            text=error_message,
            parse_mode='MarkdownV2'
        )


async def scheduled_monthly_task(context: ContextTypes.DEFAULT_TYPE):
    """
    Проверяет, наступил ли нужный день месяца, и запускает основную задачу.
    """
    tz = pytz.timezone(config.TZ_INFO)
    current_day = datetime.now(tz).day

    logger.debug(
        f"Ежедневная проверка для ежемесячной задачи. Сегодня {current_day}-е число. Цель: {config.MONTHLY_JOB_DAY}.")

    if current_day == config.MONTHLY_JOB_DAY:
        logger.info(f"Сегодня {current_day}-е число, запускаю ежемесячный сбор данных.")
        await run_monthly_data_collection(context)
    else:
        logger.info(
            f"Сегодня {current_day}-е число, ежемесячная задача не запускается (запланирована на {config.MONTHLY_JOB_DAY}-е).")


# --- КОНЕЦ НОВЫХ ФУНКЦИЙ ---


async def _create_holidays_message(target_date: date) -> Optional[str]:
    """
    Формирует текстовое сообщение о праздниках на указанную дату, включая регионы.
    """
    try:
        target_date_str = target_date.strftime('%Y-%m-%d')
        target_date_formatted = target_date.strftime('%d.%m.%Y')

        holiday_service = HolidayService()
        holidays_by_country = holiday_service.get_holidays_for_date(target_date_str)

        escaped_date = escape_markdown(target_date_formatted, version=2)

        if not holidays_by_country:
            return f"🗓️ На {escaped_date} праздников не найдено\\."

        message_parts = [f"🎉 *Праздники на {escaped_date}* 🎉\n"]
        for country_code in sorted(holidays_by_country.keys()):
            escaped_country_name = escape_markdown(country_code.upper(), version=2)
            message_parts.append(f"\n*{escaped_country_name}*")

            holiday_details = holidays_by_country[country_code]

            for holiday_name, regions in holiday_details.items():
                escaped_holiday_name = escape_markdown(holiday_name, version=2)
                if regions:
                    escaped_regions = escape_markdown(", ".join(regions), version=2)
                    message_parts.append(f"  \\- {escaped_holiday_name} \\- _{escaped_regions}_")
                else:
                    message_parts.append(f"  \\- {escaped_holiday_name}")

        return "\n".join(message_parts)
    except Exception as e:
        logger.error(f"Ошибка при создании сообщения о праздниках для даты {target_date}: {e}", exc_info=True)
        return None


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

    # <<< ИЗМЕНЕНИЕ: Создаем объект персистентности
    # Данные будут сохраняться в файл 'bot_persistence.pickle'
    persistence = PicklePersistence(filepath="bot_persistence.pickle")
    # <<< КОНЕЦ ИЗМЕНЕНИЯ

    # <<< ИЗМЕНЕНИЕ: Передаем объект persistence в ApplicationBuilder
    application = (
        ApplicationBuilder()
        .token(config.TELEGRAM_BOT_TOKEN)
        .persistence(persistence)
        .post_init(post_init)
        .build()
    )
    # <<< КОНЕЦ ИЗМЕНЕНИЯ

    job_queue = application.job_queue
    tz = pytz.timezone(config.TZ_INFO)

    # <<< ИЗМЕНЕНИЕ: Добавлена проверка на существование задачи перед ее созданием
    # Планировщик ежедневных уведомлений
    daily_job_name = "daily_holiday_notification"
    if config.TELEGRAM_CHANNEL_ID and config.DAILY_NOTIFICATION_TIME:
        # Проверяем, нет ли уже такой задачи (она могла быть восстановлена из файла)
        if not job_queue.get_jobs_by_name(daily_job_name):
            try:
                h, m = map(int, config.DAILY_NOTIFICATION_TIME.split(':'))
                notification_time = time(h, m, tzinfo=tz)
                job_queue.run_daily(
                    send_daily_holidays_notification,
                    time=notification_time,
                    name=daily_job_name  # Используем имя для идентификации
                )
                logger.info(
                    f"Запланирована ежедневная отправка уведомлений в {config.DAILY_NOTIFICATION_TIME} ({config.TZ_INFO}).")
            except (ValueError, TypeError) as e:
                logger.error(f"Неверный формат времени DAILY_NOTIFICATION_TIME: {e}")
        else:
            logger.info(f"Задача '{daily_job_name}' уже была восстановлена из persistence файла.")

    # Планировщик ежемесячного сбора данных
    monthly_job_name = "monthly_data_collection_job"
    if config.TELEGRAM_CHANNEL_ID and config.MONTHLY_JOB_ENABLED:
        # Проверяем, нет ли уже такой задачи
        if not job_queue.get_jobs_by_name(monthly_job_name):
            try:
                h, m = map(int, config.MONTHLY_JOB_TIME.split(':'))
                job_time = time(h, m, tzinfo=tz)
                job_queue.run_daily(
                    scheduled_monthly_task,
                    time=job_time,
                    name=monthly_job_name  # Используем имя для идентификации
                )
                logger.info(
                    f"Запланирована ежемесячная задача сбора данных на {config.MONTHLY_JOB_DAY}-е число каждого месяца "
                    f"в {config.MONTHLY_JOB_TIME} ({config.TZ_INFO})."
                )
            except (ValueError, TypeError) as e:
                logger.error(f"Неверный формат времени MONTHLY_JOB_TIME: {e}")
        else:
            logger.info(f"Задача '{monthly_job_name}' уже была восстановлена из persistence файла.")
    # <<< КОНЕЦ ИЗМЕНЕНИЯ

    holiday_check_conv_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.TEXT & filters.Regex(f'^{BTN_GET_HOLIDAYS}$'), start_holiday_check_conversation)],
        states={GET_SPECIFIC_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_specific_date)]},
        fallbacks=[CommandHandler('cancel', cancel_conversation), CommandHandler('start', start)],
        # <<< ИЗМЕНЕНИЕ: Добавляем персистентность в диалоги
        persistent=True,
        name="holiday_check_conv"
        # <<< КОНЕЦ ИЗМЕНЕНИЯ
    )

    report_conv_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.TEXT & filters.Regex(f'^{BTN_CREATE_REPORT}$'), start_report_conversation)],
        states={
            GET_START_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_start_date)],
            GET_END_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_end_date)],
        },
        fallbacks=[CommandHandler('cancel', cancel_conversation), CommandHandler('start', start)],
        # <<< ИЗМЕНЕНИЕ: Добавляем персистентность в диалоги
        persistent=True,
        name="report_conv"
        # <<< КОНЕЦ ИЗМЕНЕНИЯ
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(holiday_check_conv_handler)
    application.add_handler(report_conv_handler)

    application.run_polling()
    logger.info("Бот остановлен.")


if __name__ == "__main__":
    main()