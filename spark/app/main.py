from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

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

# bucket name
BUCKET_NAME = 'abd-tp/parquet_data_lite/'

def read_parquet_file(name: str):
    return spark.read.parquet(f"gs://{BUCKET_NAME}/{name}.parquet")

def main():        
    # data frames
    votesTypes = read_parquet_file("VotesTypes")
    print(votesTypes.columns)


if __name__ == "__main__":
    main()