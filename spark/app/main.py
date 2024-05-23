from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as sf
from typing import List
import time
from functools import wraps
import sys

# bucket name
BUCKET_NAME = 'abd-tp/parquet_data_lite/'

# utility to measure the runtime of some function
def timeit(f):
    @wraps(f)
    def wrap(*args, **kw):
        t = time.time()
        result = f(*args, **kw)
        print(f'{f.__name__}: {round(time.time() - t, 3)}s')
        return result
    return wrap


def count_rows(iterator):
    yield len(list(iterator))


# show the number of rows in each partition
def showPartitionSize(df: DataFrame):
    for partition, rows in enumerate(df.rdd.mapPartitions(count_rows).collect()):
        print(f'Partition {partition} has {rows} rows')


def read_parquet_file(spark: SparkSession, name: str) -> DataFrame:
    return spark.read.parquet(f"gs://{BUCKET_NAME}/{name}.parquet")


@timeit
def q1(votesTypes: DataFrame) -> List[Row]:
    pass


@timeit
def q2() -> List[Row]:
    pass


def main():   
    @timeit
    def w1():
        df = votesTypes
        showPartitionSize(df)
        q1(df)

    @timeit
    def w2():
        df = votesTypes.repartition(3)
        showPartitionSize(df)
        q1(df)
    
    if len(sys.argv) < 2:
        print('Missing function name. Usage: python3 main.py <function-name>')
        return
    elif sys.argv[1] not in locals():
        print(f'No such function: {sys.argv[1]}')
        return

    # the spark session
    spark = SparkSession.builder \
        .master("spark://spark:7077") \
        .config("spark.jars", "/app/gcs-connector-hadoop3-2.2.21.jar") \
        .config("spark.driver.extraClassPath", "/app/gcs-connector-hadoop3-2.2.21.jar") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "/tmp/spark-events") \
        .getOrCreate()
        # .config("spark.executor.memory", "1g") \

    # google cloud service account credentials file
    spark._jsc.hadoopConfiguration().set(
        "google.cloud.auth.service.account.json.keyfile",
        "/app/credentials.json")

    # data frames
    votesTypes = read_parquet_file(spark, "VotesTypes")


    locals()[sys.argv[1]]()


if __name__ == "__main__":
    main()