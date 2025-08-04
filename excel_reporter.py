# excel_reporter.py

import sqlite3
import os
from datetime import date
from collections import defaultdict
import openpyxl
from openpyxl.styles import Font, Alignment
from typing import List, Dict

import config

logger = config.get_logger(__name__)


def _fetch_holidays_data(country_code: str, start_date: str, end_date: str) -> List[Dict]:
    """Извлекает и агрегирует данные о праздниках для страны за указанный период."""
    log_ctx = {'country': country_code, 'start_date': start_date, 'end_date': end_date}
    logger.info("Запрос данных из БД для указанного периода...", extra={'context': log_ctx})

    # ИЗМЕНЕНО: Запрос теперь использует BETWEEN для диапазона дат
    query = """
        SELECT
            h.holiday_name,
            h.holiday_date,
            r.region_name
        FROM holidays h
        LEFT JOIN regions r ON h.id = r.holiday_id
        WHERE
            h.country_code = ? AND
            h.holiday_date BETWEEN ? AND ?
        ORDER BY
            h.holiday_date, h.holiday_name;
    """

    try:
        with sqlite3.connect(config.DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(query, (country_code, start_date, end_date)) # ИЗМЕНЕНО
            rows = cursor.fetchall()
    except sqlite3.Error as e:
        logger.exception("Ошибка при доступе к БД", extra={'context': log_ctx})
        return []

    # Агрегация данных (логика осталась прежней)
    holidays_aggregated = defaultdict(list)
    for name, dt, region in rows:
        if region:
            holidays_aggregated[(name, dt)].append(region)

    result = [
        {"name": name, "date": dt, "regions": ", ".join(sorted(regions))}
        for (name, dt), regions in holidays_aggregated.items()
    ]
    all_holidays_in_rows = {(name, dt) for name, dt, _ in rows}
    for name, dt in all_holidays_in_rows:
        if (name, dt) not in holidays_aggregated:
            result.append({"name": name, "date": dt, "regions": "Вся страна"})

    logger.info(f"Найдено {len(result)} уникальных праздников", extra={'context': log_ctx})
    return sorted(result, key=lambda x: x['date'])


# ИЗМЕНЕНО: Функция теперь принимает start_date и end_date
def generate_holidays_report(start_date: str, end_date: str) -> str:
    """
    Создает Excel-отчет по праздникам за указанный период и возвращает путь к файлу.
    """
    log_ctx = {'start_date': start_date, 'end_date': end_date, 'report_type': 'excel'}
    logger.info("Начало генерации Excel отчета для периода...", extra={'context': log_ctx})

    if not os.path.exists(config.REPORTS_DIR):
        os.makedirs(config.REPORTS_DIR)

    # ИЗМЕНЕНО: Имя файла теперь отражает период
    report_filename = f"holidays_report_{start_date}_to_{end_date}.xlsx"
    report_path = os.path.join(config.REPORTS_DIR, report_filename)

    workbook = openpyxl.Workbook()
    sheet = workbook.active
    # ИЗМЕНЕНО: Заголовок листа
    sheet.title = f"Holidays {start_date} to {end_date}"

    # Стили (без изменений)
    header_font = Font(bold=True, size=12)
    country_font = Font(bold=True, size=11)

    # Заголовки и ширина колонок (без изменений)
    headers = ["Страна", "Название праздника", "Дата", "Регионы"]
    sheet.append(headers)
    for col_num, header_title in enumerate(headers, 1):
        cell = sheet.cell(row=1, column=col_num)
        cell.font = header_font
        cell.alignment = Alignment(horizontal='center')
    sheet.column_dimensions['A'].width = 15
    sheet.column_dimensions['B'].width = 40
    sheet.column_dimensions['C'].width = 15
    sheet.column_dimensions['D'].width = 50

    current_row = 2

    for country_code in config.COUNTRIES:
        # ИЗМЕНЕНО: Передаем даты в функцию
        holidays = _fetch_holidays_data(country_code, start_date, end_date)

        country_cell = sheet.cell(row=current_row, column=1, value=country_code.upper())
        country_cell.font = country_font

        if not holidays:
            sheet.cell(row=current_row, column=2, value="За указанный период праздников не найдено")
            current_row += 1
        else:
            for i, holiday in enumerate(holidays):
                sheet.cell(row=current_row + i, column=2, value=holiday['name'])
                sheet.cell(row=current_row + i, column=3, value=holiday['date'])
                sheet.cell(row=current_row + i, column=4, value=holiday['regions'])
            current_row += len(holidays)
        current_row += 1

    workbook.save(report_path)
    logger.info(f"Excel отчет успешно сохранен: {report_path}", extra={'context': log_ctx})
    return report_path