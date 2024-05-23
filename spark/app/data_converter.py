from pyspark.sql import SparkSession
from multiprocessing import Pool
from pathlib import Path
import os
from pyspark.sql import functions as sf

# the spark session
spark = SparkSession.builder.master("spark://spark:7077") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

DATA_TYPE = "data_full"
# DATA_DIR = f"../../db/{DATA_TYPE}"
DATA_DIR = f"/app/{DATA_TYPE}"

FILES = [
    "Badges.csv",
    "Comments.csv",
    "Questions.csv",
    "QuestionsTags.csv",
    "QuestionsLinks.csv",
    "Answers.csv",
    "Tags.csv",
    "Users.csv",
    "Votes.csv",
    "VotesTypes.csv"
]


def convert_to_parquet(file_path, filename):
    print(f'Converting {filename} ({file_path})')
    data = spark.read.csv(file_path, header=True, inferSchema=True, multiLine=True, escape='\"')
    #data = spark.read.csv(file_path, header=True, inferSchema=True, sep=",")
    parquet_file = f"/app/parquet_{DATA_TYPE}/{filename}.parquet"
    print(f'Parquet file: {parquet_file}')
    if filename.lower() in ["questions", "questionslinks", "users", "answers", "comments", "votes"]:
        #data = data.withColumn("creationday", sf.to_date(sf.col("creationdate")))
        data.write.mode("overwrite").parquet(parquet_file, compression="gzip") #, partitionBy="creationday")
    else:
        data.write.mode("overwrite").parquet(parquet_file, compression="gzip")

    print(f'Converted {filename} ({file_path})')

def load_files():
    # p = Pool(os.cpu_count())
    # jobs = []
    for file_path in [os.path.join(DATA_DIR, x) for x in FILES]:
        filename = Path(file_path).stem
        convert_to_parquet(file_path, filename)
        # jobs.append(p.apply_async(convert_to_parquet, (file_path, filename)))
    # [j.get() for j in jobs]

if __name__ == "__main__":
    load_files()