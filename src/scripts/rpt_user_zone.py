import sys
import logging
import pyspark.sql.functions as F 
import datetime as dt
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

# Создание отчета по зонам пользователей
def user_zone_report() -> None:
    try:
        # Получение аргументов командной строки
        date = sys.argv[1]
        dir_name_from = sys.argv[2]
        dir_name_to = sys.argv[3]

        # Создание SparkSession
        spark = SparkSession\
            .builder.appName(f"UserZoneReport-{date}")\
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
        
        # Упорядочивание сообщений пользователей по времени
        ordered_user_cities = message_data\
            .select(F.col('event.message_from').alias('user_id'), F.col('nearest_city').alias('act_city'), 'event.message_ts')\
            .withColumn(
                "rank", 
                F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("message_ts")))
            )
        
        ordered_user_cities.createOrReplaceTempView('ordered_user_cities')
        
        logging.info("ordered_user_cities был успешно создан")
        
        # Получение текущего города пользователей
        current_city = spark.sql("SELECT user_id, act_city FROM ordered_user_cities WHERE rank = 1")
        
        logging.info("current_city был успешно создан")
        
        # Получение времени последнего сообщения
        last_message_ts = spark.sql("SELECT MAX(message_ts) as max_ts FROM ordered_user_cities").collect()[0][0]
        
        # Обработка данных перемещений пользователей
        shift_data = spark.sql("SELECT * FROM ordered_user_cities")\
            .withColumn(
                "lag_act_city_-1", 
                F.lag('act_city', -1, 'start').over(Window.partitionBy("user_id").orderBy(F.desc("message_ts")))
            )\
            .withColumn('eq_act_city', F.col('act_city') == F.col('lag_act_city_-1'))\
            .where('eq_act_city is false')
        
        shift_data.createOrReplaceTempView('shift_data')
        
        logging.info("shift_data был успешно создан")

        # Определение домашнего города пользователя
        home_city = spark.sql("SELECT * FROM shift_data")\
            .withColumn(
                "lag_message_ts_+1", 
                F.lag('message_ts', 1, last_message_ts).over(Window.partitionBy("user_id").orderBy(F.desc("message_ts")))
            )\
            .withColumn('duration_days', (F.to_timestamp("lag_message_ts_+1").cast("long") - F.to_timestamp('message_ts').cast("long")) / (24*3600))\
            .where('duration_days > 27')\
            .withColumn(
                "rank_home_city", 
                F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("message_ts")))
            )\
            .where('rank_home_city = 1')\
            .join(geo_data.select(F.col('city').alias('act_city'), F.col('timezone').alias('timezone_copy')), on='act_city', how='left')\
            .withColumn('timezone', F.concat(F.lit('Australia/'), F.col('timezone_copy')))\
            .withColumn('local_time', F.from_utc_timestamp(F.col("message_ts"), F.col('timezone')))\
            .select('user_id', F.col('act_city').alias('home_city'), 'local_time')
        
        logging.info("home_city был успешно создан")
            
        # Определение данных о перемещениях пользователей
        travel_data = spark.sql("SELECT user_id, act_city FROM shift_data")\
            .groupBy('user_id')\
            .agg(F.count("act_city").alias('travel_count'), F.collect_list("act_city").alias('travel_array'))
        
        logging.info("travel_data был успешно создан")
        
        # Удаление временных представлений
        spark.catalog.dropTempView("shift_data")
        spark.catalog.dropTempView("ordered_user_cities")
        
        # Формирование итогового отчета
        report = current_city\
            .join(home_city, on='user_id', how='fullouter')\
            .join(travel_data, on='user_id', how='fullouter')
        
        logging.info("report был успешно создан")

        # Запись отчета
        report.write.mode("overwrite").parquet(dir_name_to)
        
        logging.info(f"Отчет был успешно записан в {dir_name_to}")
        
    except Exception as e:
        logging.exception(f"Ошибка: {e}")
    finally:
        spark.stop()

if __name__ == '__main__':
    user_zone_report()
