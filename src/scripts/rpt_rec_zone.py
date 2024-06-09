import sys
import logging
import pyspark.sql.functions as F 
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException, IllegalArgumentException

# Вычисление расстояния между двумя точками по их широте и долготе
def dist_count(lat1, lat2, lon1, lon2):
    part1 = F.pow(F.sin((lat2 - lat1) / F.lit(2)), 2)
    part2 = F.cos(lat1)
    part3 = F.cos(lat2)
    part4 = F.pow(F.sin((lon2 - lon1) / F.lit(2)), 2)
    
    dist = F.lit(2 * 6371) * F.asin(F.sqrt(part1 + (part2 * part3 * part4)))
    
    return dist

# Создания отчета по зонам рекомендаций
def recommendation_zone_report() -> None:
    try:
        # Получение аргументов командной строки
        date = sys.argv[1]
        dir_name_from = sys.argv[2]
        dir_name_to = sys.argv[3]

        # Создание SparkSession
        spark = SparkSession\
            .builder.appName(f"RecommendationZoneReport-{date}")\
            .config("spark.dynamicAllocation.enabled", "true")\
            .getOrCreate()

        logging.info("SparkSession был успешно создан")
        
        # Загрузка географических данных
        geo_data = spark.read\
            .parquet('hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/islamiang/data/geo/city_coordinates')
        
        # Загрузка данных сообщений
        message_data = spark.read\
            .parquet(dir_name_from)\
            .where(f"event_type='message' and nearest_city is not null and date <= '{date}'")
        
        logging.info("Данные сообщений успешно загружены")
        
        # Получение уникальных пользователей
        users = message_data.select(F.col('event.message_from').alias('user')).distinct()
        
        # Создание пар пользователей
        user_pairs = users.crossJoin(users.select(F.col('user').alias('user_right')))\
            .where('user != user_right')\
            .select(F.array_sort(F.array("user", "user_right")).alias('user_pair'))\
            .distinct()\
            .select(F.col('user_pair')[0].alias('user_left'), F.col('user_pair')[1].alias('user_right'))
        
        logging.info("Пары пользователей успешно загружены")
        
        # Получение последних сообщений пользователей
        last_messages = message_data\
            .select('event.message_from', 'lat', 'lon', 'event.message_ts', 'nearest_city')\
            .withColumn(
                "rank", 
                F.row_number().over(Window.partitionBy("event.message_from").orderBy(F.desc("event.message_ts")))
            )\
            .where('rank = 1')
        
        logging.info("Последние сообщения пользователей успешно загружены")
        
        # Загрузка данных подписок
        subscription_data = spark.read\
            .parquet(dir_name_from)\
            .where("event_type='subscription'")\
            .select('event.user', 'event.subscription_channel')\
            .distinct()\
            .groupBy('user')\
            .agg(F.collect_list("subscription_channel").alias('subscription_channels'))
        
        logging.info("Данные подписок успешно загружены")
        
        # Формирование отчета
        report = user_pairs\
            .join(last_messages.select(F.col('event.message_from').alias('user_left'), F.col('lat').alias('user_left_lat'), F.col('lon').alias('user_left_lon'), F.col('nearest_city').alias('zone_id')), on='user_left', how='left')\
            .join(last_messages.select(F.col('event.message_from').alias('user_right'), F.col('lat').alias('user_right_lat'), F.col('lon').alias('user_right_lon')), on='user_right', how='left')\
            .withColumn('distance', dist_count(F.col('user_left_lat'), F.col('user_right_lat'), F.col('user_left_lon'), F.col('user_right_lon')))\
            .where("distance <= 1000")\
            .select('user_left', 'user_right', 'zone_id')\
            .join(subscription_data.select(F.col('user').alias('user_left'), F.col('subscription_channels').alias('user_left_channels')), on='user_left', how='left')\
            .join(subscription_data.select(F.col('user').alias('user_right'), F.col('subscription_channels').alias('user_right_channels')), on='user_right', how='left')\
            .withColumn('channels_intersect', F.array_intersect('user_left_channels', 'user_right_channels'))\
            .filter(F.size(F.col('channels_intersect')) >= 1)\
            .withColumn("processed_dttm", F.current_date())\
            .join(geo_data.select(F.col('city').alias('zone_id'), F.col('timezone')), on='zone_id', how='left')\
            .withColumn('timezone_city', F.concat(F.lit('Australia/'), F.col('timezone')))\
            .withColumn('local_time', F.from_utc_timestamp(F.current_timestamp(), F.col('timezone_city')))\
            .select('user_left', 'user_right', 'processed_dttm', 'zone_id', 'local_time')
        
        logging.info("Отчет успешно сформирован")
        
        # Запись отчета
        report.write.mode("overwrite").parquet(dir_name_to)
        
        logging.info(f"Отчет был успешно записан в {dir_name_to}")
        
    except (ValueError, FileNotFoundError, AnalysisException, IllegalArgumentException) as e:
        logging.error(f"Произошла ошибка: {e}")
    except Exception as e:
        logging.exception("Произошла необработанная ошибка!")
    finally:
        spark.stop()


if __name__ == '__main__':
    recommendation_zone_report()
