from pyspark.sql import SparkSession
import os
import requests


def download_data(file_name):
    """
    Завантажує CSV-файл із вказаного URL-адресу та зберігає його локально.

    Параметри:
    file_name (str): Назва файлу без розширення, який потрібно завантажити.

    Вихід:
    Якщо завантаження неуспішне, функція завершить виконання з повідомленням про помилку.
    """
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + file_name + ".csv"
    print(f"Downloading from {downloading_url}")  # Виведення повідомлення про URL-адресу завантаження
    response = requests.get(downloading_url)  # Виконання HTTP-запиту

    if response.status_code == 200:  # Перевірка успішності запиту
        with open(f"{file_name}.csv", 'wb') as file:  # Відкриття файлу для запису у двійковому режимі
            file.write(response.content)  # Запис контенту у файл
        print(f"File downloaded successfully and saved as {file_name}.csv")  # Повідомлення про успішне завантаження
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")  # Завершення програми у разі помилки


# Створення сесії Spark для обробки даних
spark = SparkSession.builder \
    .appName("LandingToBronze") \
    .getOrCreate()

# Список таблиць, які необхідно обробити
tables = ["athlete_bio", "athlete_event_results"]

# Обробка кожної таблиці
for table in tables:
    # Формуємо локальний шлях для збереження CSV-файлу
    local_path = f"{table}.csv"
    # Завантаження файлу з сервера
    download_data(table)

    # Читання CSV-файлу у DataFrame Spark
    df = spark.read.csv(local_path, header=True, inferSchema=True)

    # Створення директорії для збереження parquet-файлів
    output_path = f"/tmp/bronze/{table}"
    os.makedirs(output_path, exist_ok=True)  # Переконуємось, що директорія існує

    # Запис DataFrame у форматі parquet з режимом "overwrite"
    df.write.mode("overwrite").parquet(output_path)

    # Повідомлення про успішне збереження
    print(f"Data saved to {output_path}")

    # Повторне читання parquet-файлу для перевірки даних
    df = spark.read.parquet(output_path)
    df.show(truncate=False)  # Виведення вмісту DataFrame без обрізання рядків

# Завершення роботи Spark-сесії
spark.stop()
