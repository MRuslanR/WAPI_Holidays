# config.py
import os
from dotenv import load_dotenv
import pandas as pd
import logging
import sys

# Загружаем переменные окружения из .env файла
load_dotenv()

# --- Секреты и API ---
API_KEY_NINJAS = os.getenv("API_KEY_NINJAS")
API_KEY_PERPLEXITY = os.getenv("API_KEY_PERPLEXITY")

# --- Telegram ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
DAILY_NOTIFICATION_TIME = "10:00"
TZ_INFO = "Europe/Moscow"

# --- SMTP ---
EMAIL_NOTIFICATIONS_ENABLED = True
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

# --- Nikta.ai API ---
NIKTA_BASE_URL = "https://wapi.nikta.ai/llm/api"
NIKTA_USER_EMAIL = os.getenv("NIKTA_USER_EMAIL")
NIKTA_USER_PASSWORD = os.getenv("NIKTA_USER_PASSWORD")
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30
NIKTA_DEDUPLICATE_SCENARIO_ID = 3
NIKTA_HOLIDAY_CHECKER_SCENARIO_ID = 5


# --- Общие настройки приложения ---
DB_PATH = 'holidays.db'
CONFIG_PATH = 'config.xlsx'
REPORTS_DIR = 'reports'


# Включить/выключить автоматический ежемесячный запуск
MONTHLY_JOB_ENABLED = True
# День месяца для запуска (например, 25-го числа)
MONTHLY_JOB_DAY = 20
# Время запуска в формате "ЧЧ:ММ"
MONTHLY_JOB_TIME = "8:00"

# --- Централизованная настройка логирования ---

class ContextFilter(logging.Filter):
    """ Пользовательский фильтр для добавления контекста в логи. """

    def filter(self, record):
        if not hasattr(record, 'context'):
            record.context = ''
        elif isinstance(record.context, dict) and record.context:
            record.context = ", ".join(f"{k}={v}" for k, v in record.context.items())
        else:
            record.context = str(record.context)
        return True


def setup_logging():
    """
    Настраивает корневой логгер для всего приложения.
    Должна вызываться один раз при старте.
    """
    # Убираем все существующие обработчики с корневого логгера, чтобы избежать дублирования
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(name)s] - [%(funcName)s] - [%(context)s] - %(message)s'
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    handler.addFilter(ContextFilter())

    # Настраиваем корневой логгер
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    logging.getLogger("httpx").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """ Возвращает экземпляр логгера с указанным именем. """
    return logging.getLogger(name)


# --- Загрузка конфигурации с использованием логгера ---

setup_logging()  # Настраиваем логирование до его первого использования
logger = get_logger(__name__)


def load_countries_from_config(file_path):
    """
    Загружает список стран из листа 'Countries' в файле config.xlsx.
    Возвращает список двухбуквенных кодов стран.
    """
    try:
        df = pd.read_excel(file_path, sheet_name='Countries', header=None, usecols=[0])
        countries = df[0].dropna().tolist()
        logger.info(f"✅ Страны для обработки загружены: {countries}")
        return countries
    except Exception as e:
        logger.error(f"❌ Ошибка при чтении листа 'Countries' из файла '{file_path}': {e}")
        return []


def load_emails_from_config(file_path):
    """
    Загружает список email-адресов из листа 'Emails' в файле config.xlsx.
    Возвращает список адресов.
    """
    try:
        xls = pd.ExcelFile(file_path)
        if 'Emails' not in xls.sheet_names:
            logger.warning("ℹ️ Лист 'Emails' не найден в config.xlsx. Функция отправки на почту будет недоступна.")
            return []

        df = pd.read_excel(file_path, sheet_name='Emails', header=None, usecols=[0])
        emails = df[0].dropna().tolist()
        if emails:
            logger.info(f"✅ Загружены email-адреса для рассылки: {len(emails)}.")
        return emails
    except FileNotFoundError:
        logger.warning(f"⚠️ Конфигурационный файл '{file_path}' не найден. Email-адреса не загружены.")
        return []
    except Exception as e:
        logger.error(f"❌ Ошибка при чтении листа 'Emails' из файла '{file_path}': {e}")
        return []


COUNTRIES = load_countries_from_config(CONFIG_PATH)
EMAIL_RECIPIENTS = load_emails_from_config(CONFIG_PATH)

# --- Проверка обязательных параметров ---
REQUIRED_VARS = [
    "API_KEY_NINJAS", "API_KEY_PERPLEXITY", "NIKTA_USER_EMAIL",
    "NIKTA_USER_PASSWORD", "SMTP_SERVER", "SMTP_PORT", "SMTP_USER",
    "SMTP_PASSWORD", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHANNEL_ID"
]

missing_vars = [var for var in REQUIRED_VARS if not globals().get(var)]
if missing_vars:
    error_message = f"Ошибка: Отсутствуют обязательные переменные окружения в .env: {', '.join(missing_vars)}"
    logger.critical(error_message)
    raise EnvironmentError(error_message)
