import pickle
from urlextract import URLExtract
import requests
from loguru import logger
from datetime import datetime, timedelta
import os
from multiprocessing import Pool, freeze_support
import time


# Перевірка та створення папки для логів
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

# Конфігурація логера
logger.add(
    f"{log_dir}/log_{datetime.now():%Y-%m-%d_%H-%M}.log",
    rotation="5 minutes",
    level="INFO"
)

# Читаємо файл
def read_file(filename):
    try:
        with open(filename, 'rb') as file:
            messages = pickle.load(file)
        return messages
    except FileNotFoundError:
        logger.error(f"Файл {filename} не знайдено")
        return []

# Парсим URL
def extract_urls(messages):
    extractor = URLExtract()
    urls = []
    for message in messages:
        try:
            extracted_urls = extractor.find_urls(message)
            urls.extend(extracted_urls)
        except Exception as e:
            logger.error(f"Помилка під час парсингу URL: {e}")
    logger.info(f"Отримано {len(urls)} URL з файлу")
    return urls

# Чекаєм статуси URL
def check_url(url):
    try:
        response = requests.head(url)
        logger.info(f"URL {url} перевірено. Статус: {response.status_code}")
        return url, response.status_code
    except requests.exceptions.RequestException as e:
        logger.error(f"Помилка при перевірці URL {url}: {e}")
        return url, None


# Видалення старих логів 
def delete_old_logs(log_dir):
    current_time = datetime.now()
    for file_name in os.listdir(log_dir):
        file_path = os.path.join(log_dir, file_name)
        if os.path.isfile(file_path):
            creation_time = datetime.fromtimestamp(os.path.getctime(file_path))
            if current_time - creation_time > timedelta(minutes=20):
                os.remove(file_path)


if __name__ == '__main__':
    freeze_support()

    filename = 'messages_to_parse.dat'
    messages = read_file(filename)

    if messages:
        start_time = time.time()
        urls = extract_urls(messages)
        with Pool() as pool:
            url_status = dict(pool.map(check_url, urls))
            pool.apply_async(delete_old_logs("logs"))
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Час виконання програми: {timedelta(seconds=execution_time)}")
        print(f"Кількість перевірених URL: {len(url_status)}")
        print(f"Словник з url-статусами {url_status}")
