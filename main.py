import logging
from datetime import date
import calendar

import config
from services import HolidayService

# Сначала настраиваем логирование
config.setup_logging()
# Затем получаем экземпляр логгера для этого файла
logger = logging.getLogger(__name__)


def get_next_date():
    """Рассчитывает даты для следующего месяца."""
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
    return '2025', '08', '2025-08-01', '2025-08-31'
    return str(year), str(next_month).zfill(2), first_day, last_day


def main():
    """
    Основная точка входа в приложение.
    Оркестрирует работу сервисов по сбору данных.
    """
    logger.info("Запуск скрипта сбора информации...")

    countries_for_holidays = config.COUNTRIES
    if not countries_for_holidays:
        logger.warning("Список стран для сбора праздников пуст. Обработка пропускается.")
    else:
        logger.info(f"Страны для обработки: {countries_for_holidays}")

        year, next_month, first_day, last_day = get_next_date()
        logger.info(f"Целевой период: {next_month}/{year} (с {first_day} по {last_day})")

        holiday_service = HolidayService()

        # Запускаем обработку для каждой страны
        for country_code in countries_for_holidays:
            try:
                holiday_service.process_holidays_for_period(
                    country_code=country_code,
                    year=year,
                    month=next_month,
                    first_day=first_day,
                    last_day=last_day
                )
            except Exception as e:
                logger.critical(f"Критическая ошибка при обработке страны {country_code}: {e}", exc_info=True)


        # Выводим финальный отчет по всем странам через логгер
        logger.info("=" * 40)
        logger.info("Общие итоги по экономике за весь запуск:")
        logger.info(f"  - Всего потрачено токенов: {holiday_service.grand_total_tokens}")
        logger.info(f"  - Итоговая стоимость: {holiday_service.grand_total_price:.4f}$")
        logger.info("=" * 40)

    logger.info("Сбор данных завершен.")


if __name__ == "__main__":
    main()