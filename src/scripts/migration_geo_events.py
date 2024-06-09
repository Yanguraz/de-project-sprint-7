import sys
import logging
import pyspark.sql.functions as F 
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException, IllegalArgumentException

# Константа радиуса Земли в километрах
EARTH_RADIUS_KM = 6371

# Преобразование строки в число с плавающей точкой
def to_float(n) -> float:
    return float(n.replace(',', '.'))

# Вычисление расстояния между двумя точками по их широте и долготе
def calculate_distance(lat1, lat2, lon1, lon2):
    part1 = F.pow(F.sin((lat2 - lat1) / F.lit(2)), 2)
    part2 = F.cos(lat1)
    part3 = F.cos(lat2)
    part4 = F.pow(F.sin((lon2 - lon1) / F.lit(2)), 2)
    
    distance = F.lit(2 * EARTH_RADIUS_KM) * F.asin(F.sqrt(part1 + (part2 * part3 * part4)))
    
    return distance

# Обновление расстояний
def update_distances() -> None:
    try:
        # Получение аргументов командной строки
        date = sys.argv[1]
        source_dir = sys.argv[2]
        target_dir = sys.argv[3]
        
        # Создание SparkSession
        spark = SparkSession\
            .builder.appName(f"MigrateGeoEvents-{date}")\
            .config("spark.dynamicAllocation.enabled", "true")\
            .getOrCreate()
        
        logging.info("SparkSession был успешно создан")
        
        # Загрузка данных событий
        event_data = spark.read.parquet(source_dir).where(F.col("date") == date)
        
        if event_data.rdd.isEmpty():
            logging.info("Нет данных для указанной даты.")
            return
        
        logging.info("Данные событий успешно загружены")
        
        # Загрузка географических данных
        geo_data = spark.read.parquet(
            'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/islamiang/data/geo/city_coordinates'
        ).collect()
        
        logging.info("Географические данные успешно загружены")
        
        # Вычисление расстояний до каждого города и добавление новых столбцов
        for geo_row in geo_data:
            city = geo_row['city']
            lat = to_float(geo_row['lat'])
            lng = to_float(geo_row['lng'])
            
            event_data = event_data.withColumn(
                city, 
                calculate_distance(
                    lat1=F.lit(lat),
                    lat2=F.col('lat'),
                    lon1=F.lit(lng),
                    lon2=F.col('lon')
                )
            )
        
        city_columns = [geo_row['city'] for geo_row in geo_data]
        
        # Определение ближайшего города
        event_data = event_data.withColumn("dist_to_nearest_city", F.least(*city_columns))
        
        conditions = [F.when(F.col(city) == F.col("dist_to_nearest_city"), F.lit(city)) for city in city_columns]
        event_data = event_data.withColumn("nearest_city", F.coalesce(*conditions))
        
        # Удаление временных столбцов
        event_data = event_data.drop(*city_columns)
        
        logging.info("Данные событий успешно обновлены")
        
        # Запись обновленных данных
        event_data.write\
            .mode("overwrite")\
            .partitionBy('event_type')\
            .parquet(f"{target_dir}/date={date}")
        
        logging.info(f"{target_dir}/date={date} был записан")
        
    except (ValueError, FileNotFoundError, AnalysisException, IllegalArgumentException) as e:
        logging.error(f"Произошла ошибка: {e}")
    except Exception as e:
        logging.exception("Произошла необработанная ошибка!")
    finally:
        spark.stop()

if __name__ == '__main__':
    update_distances()
