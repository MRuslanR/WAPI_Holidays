# email_sender.py
import smtplib
import sqlite3
import calendar
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from collections import defaultdict
from typing import Dict, List, Any, Tuple

import config

logger = config.get_logger(__name__)


def get_next_month_dates() -> Tuple[int, str, str, str]:
    """
    Определяет год, следующий месяц и формирует даты начала и конца
    следующего месяца в формате ISO (YYYY-MM-DD), а также название месяца.
    """
    today = date.today()
    current_year = today.year
    if today.month == 12:
        next_month = 1
        year = current_year + 1
    else:
        next_month = today.month + 1
        year = current_year

    last_day_of_month = calendar.monthrange(year, next_month)[1]
    first_day = date(year, next_month, 1).isoformat()
    last_day = date(year, next_month, last_day_of_month).isoformat()

    # Используем locale-независимый способ получения названия месяца
    month_names = ["", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                   "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
    month_name = month_names[next_month]

    return year, month_name, first_day, last_day


def fetch_holidays_for_period(db_path: str, start_date: str, end_date: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Извлекает праздники из БД за период и группирует их по странам.
    Также группирует регионы для каждого праздника.
    """
    log_ctx = {'db_path': db_path, 'period': f"{start_date} to {end_date}"}
    logger.info("Извлечение праздников из БД для email-рассылки...", extra={'context': log_ctx})

    # Сначала соберем все праздники с их регионами
    holidays_with_regions = defaultdict(lambda: {'details': {}, 'regions': []})
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            query = """
            SELECT h.id, h.country_code, h.holiday_date, h.holiday_name, r.region_name
            FROM holidays h
            LEFT JOIN regions r ON h.id = r.holiday_id
            WHERE h.holiday_date BETWEEN ? AND ?
            ORDER BY h.country_code, h.holiday_date, h.holiday_name;
            """
            cursor.execute(query, (start_date, end_date))
            for row in cursor.fetchall():
                holiday_id, country, hdate, name, region = row
                if not holidays_with_regions[holiday_id]['details']:
                    holidays_with_regions[holiday_id]['details'] = {
                        'country_code': country,
                        'date': hdate,
                        'name': name
                    }
                if region:
                    holidays_with_regions[holiday_id]['regions'].append(region)
    except sqlite3.Error as e:
        logger.exception("Ошибка при работе с базой данных", extra={'context': log_ctx})
        return {}

    # Теперь сгруппируем по странам
    holidays_by_country = defaultdict(list)
    for holiday_id, data in holidays_with_regions.items():
        country_code = data['details']['country_code']
        holiday_info = {
            'date': data['details']['date'],
            'name': data['details']['name'],
            'regions': sorted(data['regions'])
        }
        holidays_by_country[country_code].append(holiday_info)

    # Добавим страны, у которых нет праздников в этом месяце
    for country in config.COUNTRIES:
        if country not in holidays_by_country:
            holidays_by_country[country] = []

    logger.info(f"Найдено праздников для {len(holidays_by_country)} стран.", extra={'context': log_ctx})
    return dict(sorted(holidays_by_country.items()))


def format_holidays_as_html(holidays_by_country: Dict[str, list], month_name: str, year: int) -> str:
    """Форматирует сгруппированные по странам праздники в красивое HTML-письмо."""
    styles = {
        'body': "font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 700px; margin: auto;",
        'h1': "color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px;",
        'country_block': "margin-bottom: 25px; border: 1px solid #ddd; border-radius: 8px; overflow: hidden;",
        'country_title': "background-color: #ecf0f1; padding: 12px 15px; color: #34495e; font-size: 1.3em; margin: 0;",
        'holiday_list': "list-style-type: none; padding: 0 15px 15px 15px; margin: 0;",
        'holiday_item': "border-bottom: 1px solid #eee; padding: 12px 0; ",
        'holiday_item_last': "border-bottom: none; padding: 12px 0;",
        'date': "font-weight: bold; color: #2980b9;",
        'regions': "font-style: italic; color: #7f8c8d; font-size: 0.9em; margin-top: 5px;",
        'no_holidays': "padding: 15px; color: #7f8c8d;"
    }

    html = f"""
    <html><head><meta charset="utf-8"></head><body style="{styles['body']}">
    <h1 style="{styles['h1']}">Календарь праздников на {month_name} {year}</h1>
    <p>Ниже представлен список официальных выходных дней в отслеживаемых странах на следующий месяц.</p>
    """

    for country_code, holidays in holidays_by_country.items():
        html += f"<div style=\"{styles['country_block']}\">"
        html += f"<h2 style=\"{styles['country_title']}\">{country_code.upper()}</h2>"

        if not holidays:
            html += f"<p style=\"{styles['no_holidays']}\">В этом месяце официальных праздников не найдено.</p>"
        else:
            html += f"<ul style=\"{styles['holiday_list']}\">"
            for i, holiday in enumerate(holidays):
                style = styles['holiday_item'] if i < len(holidays) - 1 else styles['holiday_item_last']
                regions_str = ""
                if holiday['regions']:
                    regions_list = ", ".join(holiday['regions'])
                    regions_str = f"<div style=\"{styles['regions']}\">Регионы: {regions_list}</div>"

                html += f"""
                <li style="{style}">
                    <span style="{styles['date']}">{holiday['date']}:</span> {holiday['name']}
                    {regions_str}
                </li>
                """
            html += "</ul>"
        html += "</div>"

    html += "</body></html>"
    return html


def _send_email(recipient: str, subject: str, html_body: str) -> bool:
    """Внутренняя функция для отправки одного письма."""
    log_ctx = {'recipient': recipient}
    logger.info("Попытка отправки письма...", extra={'context': log_ctx})
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = config.SMTP_USER
        msg['To'] = recipient
        msg.attach(MIMEText(html_body, 'html', 'utf-8'))

        with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as server:
            server.starttls()
            server.login(config.SMTP_USER, config.SMTP_PASSWORD)
            server.send_message(msg)

        logger.info("Письмо успешно отправлено.", extra={'context': log_ctx})
        return True
    except Exception:
        logger.exception("Не удалось отправить письмо.", extra={'context': log_ctx})
        return False


# --- ИЗМЕНЕНО: Функция теперь принимает параметры ---
def send_holiday_email_to_all(year: int, month_name: str, start_date: str, end_date: str) -> dict:
    """
    Основная функция модуля: готовит и рассылает письма всем получателям.
    Возвращает словарь со статусом операции.
    """
    logger.info("Запуск задачи по рассылке писем о праздниках...")

    recipients = config.EMAIL_RECIPIENTS
    if not recipients:
        logger.warning("Список email-получателей пуст. Рассылка отменена.")
        return {'success': False, 'error': 'Список получателей пуст.'}

    # 1. Получаем данные (даты теперь передаются как аргументы)
    holidays_by_country = fetch_holidays_for_period(config.DB_PATH, start_date, end_date)

    # 2. Форматируем письмо
    email_subject = f"Календарь праздников на {month_name} {year}"
    email_body_html = format_holidays_as_html(holidays_by_country, month_name, year)

    # 3. Рассылаем
    success_count = 0
    for email_address in recipients:
        if _send_email(email_address, email_subject, email_body_html):
            success_count += 1

    logger.info(f"Рассылка завершена. Успешно отправлено {success_count} из {len(recipients)} писем.")
    return {
        'success': True,
        'sent_count': success_count,
        'total_recipients': len(recipients)
    }

if __name__ == '__main__':
    # Этот блок позволяет запускать скрипт отдельно для тестирования
    logger.info("Запуск email_sender в автономном режиме для теста.")
    # Получаем даты для следующего месяца
    test_year, test_month_name, test_first_day, test_last_day = get_next_month_dates()
    # Вызываем основную функцию с полученными датами
    result = send_holiday_email_to_all(test_year, test_month_name, test_first_day, test_last_day)
    print(f"Результат тестовой рассылки: {result}")