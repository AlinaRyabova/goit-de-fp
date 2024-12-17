from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp
import os

# Створення сесії Spark для обробки даних
spark = SparkSession.builder \
    .appName("SilverToGold") \
    .getOrCreate()

# Завантаження таблиць із silver-шару
athlete_bio_df = spark.read.parquet("/tmp/silver/athlete_bio")
athlete_event_results_df = spark.read.parquet("/tmp/silver/athlete_event_results")

# Перевірка, чи існують потрібні колонки
required_columns_bio = ["athlete_id", "height", "weight", "country_noc"]
required_columns_event = ["athlete_id", "sport", "medal", "sex"]

# Перевірка наявності колонок в таблицях
missing_columns_bio = [col for col in required_columns_bio if col not in athlete_bio_df.columns]
missing_columns_event = [col for col in required_columns_event if col not in athlete_event_results_df.columns]

if missing_columns_bio:
    print(f"Missing columns in athlete_bio: {missing_columns_bio}")
if missing_columns_event:
    print(f"Missing columns in athlete_event_results: {missing_columns_event}")

# Перейменування колонок для уникнення неоднозначності при об'єднанні
athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")

# Об'єднання таблиць за колонкою "athlete_id"
joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

# Обчислення середніх значень для кожної групи
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "bio_country_noc") \
    .agg(
        avg("height").alias("avg_height"),  # Середній зріст
        avg("weight").alias("avg_weight"),  # Середня вага
        current_timestamp().alias("timestamp")  # Час виконання агрегації
    )

# Створення директорії для збереження результатів у gold-шар
output_path = "/tmp/gold/avg_stats"
os.makedirs(output_path, exist_ok=True)  # Переконуємось, що директорія існує

# Збереження оброблених даних у форматі parquet
aggregated_df.write.mode("overwrite").parquet(output_path)

# Виведення повідомлення про успішне збереження
print(f"Data saved to {output_path}")

# Повторне читання parquet-файлу для перевірки даних
df = spark.read.parquet(output_path)
df.show(truncate=False)  # Виведення вмісту DataFrame без обрізання рядків

# Завершення роботи Spark-сесії
spark.stop()
