import csv
import json
import time
import urllib.parse
from datetime import datetime

import requests

# Токен лучше вынести в переменную, чтобы при необходимости заменить в одном месте
MASTER_TOKEN = "1e7a3c9d-8f2b-4a5e-9c0d-2b6f8a1e4c7d"

# URL эндпоинта сервиса ФИАС
BASE_URL = "https://fias-public-service.nalog.ru/api/spas/v2.0/SearchAddressItem"

# Имя входного CSV-файла
INPUT_CSV = "geocod_sankt_peterburg_202601201510.csv"


def call_fias_api(address: str, session: requests.Session) -> dict | None:
    """
    Выполнить запрос к API ФИАС для одного адреса.
    Возвращает JSON-ответ как dict, либо None в случае ошибки.
    """
    # В ТЗ указан address_type=2 – используем его явно
    params = {
        "search_string": address,
        "address_type": 2,
    }

    headers = {
        "accept": "application/json",
        "master-token": MASTER_TOKEN,
    }

    try:
        # GET-запрос с параметрами
        resp = session.get(BASE_URL, params=params, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        # Логируем ошибку в консоль, но не прерываем весь процесс
        print(f"Ошибка при запросе для адреса '{address}': {e}")
        return None


def main():
    """
    Основная функция:
    - читает CSV с колонками id, address
    - по каждому адресу делает запрос к API
    - сохраняет результаты в JSON-файл
    При большом количестве строк нужно следить за лимитами:
      100 запросов/мин, 10 000 запросов/день.
    """
    results = []

    # Формируем имя выходного файла с отметкой времени
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    output_json = f"response_{ts}.json"

    # Используем сессию requests для эффективности
    with requests.Session() as session:
        with open(INPUT_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, start=1):
                addr_id = row.get("id")
                address = row.get("address")

                if not address:
                    print(f"Строка {row_num}: пустой адрес, пропускаем")
                    continue

                print(f"[{row_num}] Обрабатываем id={addr_id}, адрес='{address}'")

                data = call_fias_api(address, session)

                # Сохраняем в результат исходные поля + ответ API
                results.append(
                    {
                        "id": addr_id,
                        "address": address,
                        "api_response": data,
                    }
                )

                # Чтобы не превышать лимит 100 запросов/мин, можно немного «усыплять» скрипт.
                # При небольшом количестве строк можно уменьшить или убрать задержку.
                time.sleep(0.7)  # ~85 запросов/минуту

    # Записываем в JSON-файл в UTF-8, с отступами для читаемости
    with open(output_json, "w", encoding="utf-8") as f_out:
        json.dump(results, f_out, ensure_ascii=False, indent=2)

    print(f"Готово. Результаты сохранены в файл: {output_json}")


if __name__ == "__main__":
    main()