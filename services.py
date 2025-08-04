# services.py
import calendar
import json
import sqlite3
from typing import List, Dict, Any, Optional
from collections import defaultdict
import config

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from config import (DB_PATH, NIKTA_BASE_URL, get_logger, DEFAULT_REQUEST_TIMEOUT_SECONDS, NIKTA_USER_PASSWORD,
                    NIKTA_USER_EMAIL, NIKTA_DEDUPLICATE_SCENARIO_ID, NIKTA_HOLIDAY_CHECKER_SCENARIO_ID)
from utils import APIError, retry_on_exception

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
logger = get_logger(__name__)


class NiktaAPIClient:
    """Клиент для взаимодействия с Nikta LLM API с retry-логикой."""

    def __init__(self, email: str, password: str):
        self.base_url = NIKTA_BASE_URL
        self._email = email
        self._password = password
        self.session = requests.Session()
        self.session.verify = False
        self.session.timeout = DEFAULT_REQUEST_TIMEOUT_SECONDS

    @retry_on_exception(exceptions=(APIError, requests.RequestException))
    def authenticate(self):
        log_ctx = {'api': 'Nikta', 'operation': 'authenticate'}
        logger.info("Аутентификация...", extra={'context': log_ctx})
        payload = {"email": self._email, "password": self._password}
        try:
            response = self.session.post(f"{self.base_url}/login", json=payload)
            response.raise_for_status()
            token = response.json().get("token")
            if not token:
                raise APIError("Токен не найден в ответе сервера.")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            logger.info("Аутентификация прошла успешно.", extra={'context': log_ctx})
        except requests.RequestException as e:
            raise APIError(f"Сетевая ошибка при аутентификации: {e}")
        except (json.JSONDecodeError, KeyError) as e:
            raise APIError(f"Ошибка парсинга ответа при аутентификации: {e}")

    @retry_on_exception(exceptions=(APIError, requests.RequestException))
    def run_scenario(self, scenario_id: int, message: str, info: dict) -> dict:
        log_ctx = {'api': 'Nikta', 'scenario_id': scenario_id}
        if "Authorization" not in self.session.headers:
            raise APIError("Клиент не аутентифицирован.")

        logger.info("Запуск сценария...", extra={'context': log_ctx})
        payload = {
            "scenario_id": scenario_id, "channel_id": '1', "dialog_id": '1', "user_id": 1,
            "state": {"messages": [{"role": "human", "content": message}], "info": info}
        }
        try:
            response = self.session.post(f"{self.base_url}/run", json=payload)
            response.raise_for_status()
            logger.info("Сценарий успешно выполнен.", extra={'context': log_ctx})
            return response.json()
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                logger.error("Ошибка 401. JWT токен, вероятно, истек. Требуется повторная аутентификация.",
                             extra={'context': log_ctx})
                self.authenticate()
            raise APIError(f"HTTP ошибка: {e.response.status_code} {e.response.text}")
        except requests.RequestException as e:
            raise APIError(f"Сетевая ошибка: {e}")
        except json.JSONDecodeError:
            raise APIError(f"Не удалось декодировать JSON из ответа: {response.text}")


class HolidayService:
    """
    Класс для сбора, обработки и сохранения информации о праздниках.
    Инкапсулирует логику взаимодействия с внешними API и базой данных.
    """

    def __init__(self):
        self.db_path = DB_PATH
        self.api_key_ninjas = config.API_KEY_NINJAS
        self.session = requests.Session()
        self.session.timeout = DEFAULT_REQUEST_TIMEOUT_SECONDS
        self.logger = get_logger(self.__class__.__name__)

        # Общие счетчики для всего жизненного цикла объекта
        self.grand_total_tokens = 0
        self.grand_total_price = 0.0

        self.logger.info("Инициализация HolidayService...")
        self._init_db()

        # Для чтения данных аутентификация не нужна, но оставим для консистентности
        # если Nikta будет использоваться и для чтения
        try:
            self.nikta_client = NiktaAPIClient(NIKTA_USER_EMAIL, NIKTA_USER_PASSWORD)
            self.nikta_client.authenticate()
        except APIError as e:
            self.logger.exception(
                "Критическая ошибка: не удалось инициализировать или аутентифицировать NiktaAPIClient.")
            raise RuntimeError(f"Не удалось запустить HolidayService из-за сбоя NiktaAPIClient: {e}") from e

    def _init_db(self):
        """Инициализирует таблицы 'holidays' и 'regions' в БД, если они не существуют."""
        log_ctx = {'service': 'DB', 'operation': 'init'}
        self.logger.info("Проверка и инициализация таблиц БД...", extra={'context': log_ctx})
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA foreign_keys = ON;")
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS holidays (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    country_code TEXT NOT NULL,
                    holiday_date DATE NOT NULL,
                    holiday_name TEXT NOT NULL,
                    UNIQUE(country_code, holiday_date, holiday_name)
                )''')
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS regions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    holiday_id INTEGER NOT NULL,
                    region_name TEXT NOT NULL,
                    FOREIGN KEY (holiday_id) REFERENCES holidays (id) ON DELETE CASCADE,
                    UNIQUE(holiday_id, region_name)
                )''')
                conn.commit()
                self.logger.info("Инициализация таблиц БД успешно завершена.", extra={'context': log_ctx})
        except sqlite3.Error:
            self.logger.exception("Критическая ошибка при инициализации таблиц БД.", extra={'context': log_ctx})
            raise

    def get_holidays_for_date(self, target_date: str) -> Dict[str, Dict[str, List[str]]]:
        """
        Извлекает из БД праздники и их регионы для указанной даты.

        :param target_date: Дата в формате 'YYYY-MM-DD'.
        :return: Словарь, где ключ - код страны, а значение - другой словарь,
                 где ключ - название праздника, а значение - список регионов.
                 Пример: {'RU': {'Новый год': ['RU-MOW', 'RU-SPE'], 'Рождество': []}}
                 Пустой список регионов означает общенациональный праздник.
        """
        log_ctx = {'service': 'DB', 'operation': 'get_holidays_with_regions', 'date': target_date}
        self.logger.info(f"Запрос праздников и регионов из БД на дату {target_date}", extra={'context': log_ctx})

        # defaultdict упрощает добавление элементов в вложенные структуры
        holidays_by_country = defaultdict(lambda: defaultdict(list))

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # LEFT JOIN, чтобы включить праздники без регионов (национальные)
                # `r.region_name` будет NULL для них
                query = """
                SELECT 
                    h.country_code, 
                    h.holiday_name, 
                    r.region_name
                FROM holidays h
                LEFT JOIN regions r ON h.id = r.holiday_id
                WHERE h.holiday_date = ?
                ORDER BY h.country_code, h.holiday_name
                """
                cursor.execute(query, (target_date,))

                for country_code, holiday_name, region_name in cursor.fetchall():
                    # Добавляем регион только если он не NULL
                    if region_name:
                        holidays_by_country[country_code][holiday_name].append(region_name)
                    else:
                        # Если регион NULL, просто убеждаемся, что праздник существует в словаре
                        # с пустым списком регионов. defaultdict уже делает это за нас.
                        _ = holidays_by_country[country_code][holiday_name]

            # Преобразуем defaultdict обратно в обычный dict для чистоты
            final_result = {k: dict(v) for k, v in holidays_by_country.items()}
            self.logger.info(f"Найдено праздников для {len(final_result)} стран.", extra={'context': log_ctx})
            return final_result

        except sqlite3.Error as e:
            self.logger.exception(f"Ошибка при чтении праздников из БД на дату {target_date}",
                                  extra={'context': log_ctx})
            return {}

    # --- КОНЕЦ НОВОГО МЕТОДА ---

    def _get_from_api(self, source_name: str, url: str, **kwargs) -> List[Dict[str, Any]]:
        """Унифицированный метод для выполнения запросов к API источников."""
        log_ctx = {'source_api': source_name, 'url': url}
        self.logger.info(f"Запрос данных из {source_name}...", extra={'context': log_ctx})
        try:
            response = self.session.get(url, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.error(f"Сетевая ошибка при запросе к {source_name}: {e}", extra={'context': log_ctx})
        except json.JSONDecodeError as e:
            self.logger.error(f"Ошибка декодирования JSON от {source_name}: {e}", extra={'context': log_ctx})
        return []

    def _get_from_ninjas(self, country_code: str, year: str, month: str) -> List[Dict[str, str]]:
        """Получает праздники из API-Ninjas."""
        url = f'https://api.api-ninjas.com/v1/workingdays?country={country_code}&month={month}'
        data = self._get_from_api("API-Ninjas", url, headers={'X-Api-Key': self.api_key_ninjas})
        holidays = []
        if not data or 'non_working_days' not in data:
            return []
        for entry in data.get('non_working_days', []):
            holiday_date, reasons = entry.get('date'), entry.get('reasons')
            if holiday_date and reasons and 'weekend' not in reasons and int(holiday_date[5:7]) == int(month) and int(
                    holiday_date[:4]) == int(year):
                holidays.append({'date': holiday_date, 'name': entry.get('holiday_name', 'Unknown Holiday')})
        self.logger.info(f"Найдено {len(holidays)} праздников в API-Ninjas для {country_code}.")
        return holidays

    def _get_from_nager(self, country_code: str, year: str, month: str) -> List[Dict[str, str]]:
        """Получает праздники из Nager.Date API."""
        url = f'https://date.nager.at/api/v3/PublicHolidays/{year}/{country_code}'
        data = self._get_from_api("Nager.Date", url)
        holidays = []
        if not isinstance(data, list):
            return []
        for entry in data:
            holiday_date = entry.get('date')
            if holiday_date and int(holiday_date[5:7]) == int(month):
                holidays.append({'date': holiday_date, 'name': entry.get('name', 'Unknown Holiday')})
        self.logger.info(f"Найдено {len(holidays)} праздников в Nager.Date для {country_code}.")
        return holidays

    def _get_from_openholidays(self, country_code: str, first_day: str, last_day: str) -> List[Dict[str, str]]:
        """Получает праздники из OpenHolidaysAPI."""
        url = "https://openholidaysapi.org/PublicHolidays"
        params = {"countryIsoCode": country_code, "languageIsoCode": "EN", "validFrom": first_day, "validTo": last_day}
        data = self._get_from_api("OpenHolidaysAPI", url, params=params, headers={"accept": "text/json"})
        holidays = []
        if not isinstance(data, list):
            return []
        for entry in data:
            if entry.get('name') and entry.get('startDate'):
                holidays.append({"date": entry['startDate'], "name": entry['name'][0]['text']})
        self.logger.info(f"Найдено {len(holidays)} праздников в OpenHolidaysAPI для {country_code}.")
        return holidays

    def _parse_nikta_checker_response(self, response_text: str) -> Optional[Dict[str, Any]]:
        """Надежно извлекает JSON из ответа сценария проверки фактов."""
        log_ctx = {'service': 'NiktaParser'}
        try:
            json_start_index = response_text.find('{')
            if json_start_index == -1:
                self.logger.warning("Не найден JSON объект в ответе Nikta.", extra=log_ctx)
                return None
            if "**Источники:**" in response_text:
                json_part = response_text[:response_text.find("**Источники:**")]
            else:
                json_part = response_text
            json_end_index = json_part.rfind('}')
            if json_end_index == -1:
                self.logger.warning("Не найден корректный конец JSON объекта в ответе Nikta.", extra=log_ctx)
                return None
            clean_json_str = json_part[json_start_index: json_end_index + 1].strip()
            return json.loads(clean_json_str)
        except json.JSONDecodeError:
            self.logger.exception(f"Не удалось декодировать JSON из ответа Nikta. Ответ: '{response_text}'",
                                  extra=log_ctx)
        except Exception:
            self.logger.exception(f"Непредвиденная ошибка при парсинге ответа Nikta. Ответ: '{response_text}'",
                                  extra=log_ctx)
        return None

    def _save_verified_holiday(self, country_code: str, holiday_data: Dict[str, Any]):
        """
        Сохраняет один проверенный праздник и связанные с ним регионы в БД в рамках одной транзакции.
        """
        log_ctx = {'service': 'DB', 'operation': 'save', 'holiday_name': holiday_data.get('name')}
        self.logger.info(f"Сохранение проверенного праздника '{holiday_data.get('name')}' в БД.", extra=log_ctx)
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA foreign_keys = ON;")
                cursor.execute(
                    'INSERT OR IGNORE INTO holidays (country_code, holiday_date, holiday_name) VALUES (?, ?, ?)',
                    (country_code, holiday_data['date'], holiday_data['name'])
                )
                cursor.execute(
                    'SELECT id FROM holidays WHERE country_code = ? AND holiday_date = ? AND holiday_name = ?',
                    (country_code, holiday_data['date'], holiday_data['name'])
                )
                holiday_id_tuple = cursor.fetchone()
                if not holiday_id_tuple:
                    self.logger.error("Не удалось найти/создать запись о празднике, сохранение регионов отменено.",
                                      extra=log_ctx)
                    return
                holiday_id = holiday_id_tuple[0]
                regions = holiday_data.get('regions', [])
                if not regions:
                    self.logger.warning("У праздника нет регионов для сохранения.", extra=log_ctx)
                    return
                regions_to_insert = [(holiday_id, region_name) for region_name in regions]
                cursor.executemany(
                    'INSERT OR IGNORE INTO regions (holiday_id, region_name) VALUES (?, ?)',
                    regions_to_insert
                )
                conn.commit()
                self.logger.info(
                    f"Успешно сохранено {cursor.rowcount} новых регионов для праздника '{holiday_data['name']}'.",
                    extra=log_ctx)
        except sqlite3.Error:
            self.logger.exception(f"Ошибка при сохранении праздника '{holiday_data.get('name')}' в БД.", extra=log_ctx)
        except KeyError as e:
            self.logger.error(f"Отсутствует обязательное поле '{e}' в данных праздника для сохранения: {holiday_data}",
                              extra=log_ctx)

    def process_holidays_for_period(self, country_code: str, year: str, month: str, first_day: str, last_day: str):
        """
        Основной метод: собирает, обрабатывает и сохраняет праздники для страны за период.
        """
        log_ctx = {'country': country_code, 'period': f"{year}-{month}"}
        self.logger.info(f"Начало обработки праздников для страны: {country_code.upper()}", extra={'context': log_ctx})

        # Локальные счетчики для текущей страны
        country_tokens = 0
        country_price = 0.0

        # 1. Сбор данных из всех источников
        raw_holidays = {
            "ninjas_holidays": self._get_from_ninjas(country_code, year, month),
            "nager_holidays": self._get_from_nager(country_code, year, month),
            "open_holidays": self._get_from_openholidays(country_code, first_day, last_day)
        }
        if not any(raw_holidays.values()):
            self.logger.warning("Ни один из источников не вернул данных о праздниках. Обработка завершена.",
                                extra={'context': log_ctx})
            return

        # 2. Дедупликация через Nikta
        try:
            self.logger.info("Отправка данных на дедупликацию в Nikta...", extra={'context': log_ctx})
            dedup_result = self.nikta_client.run_scenario(NIKTA_DEDUPLICATE_SCENARIO_ID, str(raw_holidays), {})

            # Подсчет экономики для отдельного запроса
            nikta_tokens = dedup_result.get('tokens', 0)
            nikta_price = dedup_result.get('logs', {}).get('total_price', 0.0)
            self.logger.info(f"[Экономика] Запрос на дедупликацию: {nikta_tokens} токенов, {nikta_price:.4f}$")

            # Обновляем счетчики для страны и общие
            country_tokens += nikta_tokens
            country_price += nikta_price
            self.grand_total_tokens += nikta_tokens
            self.grand_total_price += nikta_price

            clean_holidays_str = dedup_result.get('result', '{}')
            clean_holidays_data = json.loads(clean_holidays_str)
            holidays_to_check = clean_holidays_data.get('holidays', [])
            self.logger.info(
                f"Дедупликация завершена. Получено {len(holidays_to_check)} уникальных праздников для проверки.",
                extra={'context': log_ctx})
        except (APIError, json.JSONDecodeError) as e:
            self.logger.exception("Ошибка на этапе дедупликации праздников. Обработка страны прервана.",
                                  extra={'context': log_ctx})
            return

        # 3. Проверка фактов и сохранение
        if not holidays_to_check:
            self.logger.info("После дедупликации не осталось праздников для проверки.", extra={'context': log_ctx})
        else:
            self.logger.info("Начало проверки фактов и сохранения праздников...", extra={'context': log_ctx})
            for holiday in holidays_to_check:
                try:
                    holiday['region'] = country_code
                    checker_result = self.nikta_client.run_scenario(NIKTA_HOLIDAY_CHECKER_SCENARIO_ID, str(holiday), {})

                    # Подсчет экономики для отдельного запроса
                    nikta_tokens = checker_result.get('tokens', 0)
                    nikta_price = checker_result.get('logs', {}).get('total_price', 0.0)
                    self.logger.info(
                        f"[Экономика] Запрос на проверку факта '{holiday.get('name')}': {nikta_tokens} токенов, {nikta_price:.4f}$.")

                    # Обновляем счетчики для страны и общие
                    country_tokens += nikta_tokens
                    country_price += nikta_price
                    self.grand_total_tokens += nikta_tokens
                    self.grand_total_price += nikta_price

                    verified_data = self._parse_nikta_checker_response(checker_result.get('result', ''))

                    if not verified_data:
                        self.logger.warning(f"Не удалось разобрать ответ от Nikta для праздника: {holiday}",
                                            extra={'context': log_ctx})
                        continue

                    is_holiday_flag = verified_data.get('is_holiday')
                    if str(is_holiday_flag).lower() == 'true':
                        self.logger.info(f"Праздник '{holiday.get('name')} - {holiday.get('date')}' является выходным.")
                        self._save_verified_holiday(country_code, verified_data)
                    else:
                        self.logger.info(
                            f"Праздник '{holiday.get('name')} - {holiday.get('date')}' НЕ является выходным.")
                except APIError:
                    self.logger.exception(f"Ошибка API при проверке праздника: {holiday}. Пропускаем.",
                                          extra={'context': log_ctx})
                except Exception:
                    self.logger.exception(f"Непредвиденная ошибка при обработке праздника: {holiday}. Пропускаем.",
                                          extra={'context': log_ctx})

        # Вывод отчета по экономике для страны через логгер
        self.logger.info(f"Итоги по экономике для страны {country_code.upper()}:")
        self.logger.info(f"  - Потрачено токенов: {country_tokens}")
        self.logger.info(f"  - Общая стоимость: {country_price:.4f}$")

        self.logger.info(f"Обработка праздников для страны {country_code.upper()} завершена.",
                         extra={'context': log_ctx})