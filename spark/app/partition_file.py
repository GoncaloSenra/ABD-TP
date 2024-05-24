from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as sf
from typing import List
import time
from functools import wraps
import sys

# bucket name
BUCKET_NAME = 'abd-tp'
READ_BUCKET_PATH = f'{BUCKET_NAME}/parquet_data_full/'
WRITE_BUCKET_PATH = f'{BUCKET_NAME}/partioned_data_full/'

# utility to measure the runtime of some function
def timeit(f):
    @wraps(f)
    def wrap(*args, **kw):
        t = time.time()
        result = f(*args, **kw)
        print(f'{f.__name__}: {round(time.time() - t, 3)}s')
        return result
    return wrap

def read_parquet_file(spark: SparkSession, name: str) -> DataFrame:
    return spark.read.parquet(f"gs://{READ_BUCKET_PATH}/{name}.parquet")

def write_parquet_file(df: DataFrame, name: str, partitionBy: str = None) -> DataFrame:
    if partitionBy is None:
        df.write.mode("overwrite").parquet(f"gs://{WRITE_BUCKET_PATH}/{name}.parquet")
    else:
        df.withColumn("dia", sf.to_date(sf.col(partitionBy))) \
          .withColumn("hora", sf.hour(sf.col(partitionBy))) \
          .write.mode("overwrite").partitionBy("dia", "hora").parquet(f"gs://{WRITE_BUCKET_PATH}/{name}.parquet")


def partion_file(spark: SparkSession, name: str, partitionBy: str = None):
    df = read_parquet_file(spark, name)
    write_parquet_file(df, name, partitionBy)

def main():   
    # the spark session
    spark = SparkSession.builder \
        .master("spark://spark:7077") \
        .config("spark.jars", "/app/gcs-connector-hadoop3-2.2.21.jar") \
        .config("spark.driver.extraClassPath", "/app/gcs-connector-hadoop3-2.2.21.jar") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "/tmp/spark-events") \
        .getOrCreate()

    # google cloud service account credentials file
    spark._jsc.hadoopConfiguration().set(
        "google.cloud.auth.service.account.json.keyfile",
        "/app/credentials.json")

    # data frames
    partion_file(spark, "Answers", "CreationDate")
    # partion_file(spark, "Badges", "Date")
    # partion_file(spark, "Comments", "CreationDate")
    # partion_file(spark, "Questions", "CreationDate")
    # partion_file(spark, "QuestionsLinks", "CreationDate")
    # partion_file(spark, "QuestionsTags")
    # partion_file(spark, "Tags")
    # partion_file(spark, "Users", "CreationDate")
    # partion_file(spark, "Votes", "CreationDate")
    # partion_file(spark, "VotesTypes")

if __name__ == "__main__":
    main()