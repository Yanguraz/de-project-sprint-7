import sys
import logging
import pyspark.sql.functions as F
import datetime as dt
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

# Создание недельного отчета по зонам
def week_zone_report() -> None:
    try:
        date = sys.argv[1]
        dir_name_from = sys.argv[2]
        dir_name_to = sys.argv[3]
        
        # Создание SparkSession
        spark = SparkSession\
            .builder.appName(f"WeekZoneReport-{date}")\
            .config("spark.dynamicAllocation.enabled", "true")\
            .getOrCreate()
        
        cur_date: dt.datetime = dt.datetime.strptime(date, '%Y-%m-%d')
        week_start: dt.datetime = cur_date - dt.timedelta(days=cur_date.weekday())
        week_end: dt.datetime = week_start + dt.timedelta(days=6)
        month_start: dt.datetime = cur_date - dt.timedelta(days=cur_date.day-1)

        logging.info("SparkSession был успешно создан")
        
        # Загрузка событий данных за месяц до указанной даты
        data_events = spark.read\
            .parquet(dir_name_from)\
            .where(f"date >= '{month_start.strftime('%Y-%m-%d')}' AND date < '{date}'")\
            .withColumn("month", F.month(F.col('date')))\
            .withColumn("week", F.weekofyear(F.col('date')))\
            .where(F.col("nearest_city").isNotNull())
        
        if data_events.rdd.isEmpty():
            logging.info("Данные событий пусты")
            return None
        
        logging.info("Данные событий были успешно загружены")

        # Определение регистрации пользователей
        registrations = data_events.where(F.col('event_type') == 'message')\
            .withColumn("reg_date_rank", F.row_number().over(Window.partitionBy('user_id').orderBy(F.asc("date"))))\
            .where(F.col('reg_date_rank') == 1).drop('reg_date_rank')\
            .select(
                "month",
                "week",
                'nearest_city'
            )\
            .withColumn('event_type', F.lit('week_user'))

        logging.info("Данные регистрации были успешно загружены")

        # Формирование недельного отчета
        data_events_week = data_events\
            .groupBy("month", "week", 'nearest_city')\
            .pivot('event_type')\
            .count()\
            .select(
                "month",
                "week",
                'nearest_city',
                F.col('message').alias('week_message'),
                F.col('reaction').alias('week_reaction'),
                F.col('subscription').alias('week_subscription')
            ).join(registrations.groupBy("month", "week", 'nearest_city').agg(F.count("*").alias('week_user')), 
                   ['month', 'week', 'nearest_city'])
        
        # Формирование месячного отчета
        data_events_month = data_events\
            .groupBy("month", 'nearest_city')\
            .pivot('event_type')\
            .count()\
            .select(
                "month",
                'nearest_city',
                F.col('message').alias('month_message'),
                F.col('reaction').alias('month_reaction'),
                F.col('subscription').alias('month_subscription')
            ).join(registrations.groupBy("month", 'nearest_city').agg(F.count("*").alias('month_user')), 
                   ['month', 'nearest_city'])

        # Объединение недельного и месячного отчетов
        report = data_events_week.join(data_events_month, on=["month", 'nearest_city'], how='left')

        logging.info("Отчет был успешно создан")
        
        # Запись отчета
        report.write\
            .mode("overwrite")\
            .parquet(dir_name_to + f"/month={date.split('-')[1]}")
        
        logging.info(f"Отчет был успешно записан в {dir_name_to}/month={date.split('-')[1]}")
        
    except Exception as e:
        logging.exception(f"Ошибка: {e}")
    finally:
        spark.stop()

if __name__ == '__main__':
    week_zone_report()
