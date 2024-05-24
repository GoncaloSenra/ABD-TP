from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as sf
from typing import List
import time
from functools import wraps
import sys

# bucket name
BUCKET_NAME = 'abd-tp/parquet_data_full/'

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
def showPartitionSize(label: str, df: DataFrame):
    print(f"{label}:")
    for partition, rows in enumerate(df.rdd.mapPartitions(count_rows).collect()):
        print(f'Partition {partition} has {rows} rows')
    print("")

def read_parquet_file(spark: SparkSession, name: str) -> DataFrame:
    return spark.read.parquet(f"gs://{BUCKET_NAME}/{name}.parquet")

def print_rows(rows: List[Row]):
    for r in rows:
        print(r)

@timeit
def q1(users: DataFrame, questions: DataFrame, answers: DataFrame, comments: DataFrame, partitions: int = None) -> List[Row]:
    
    # Define the 6 months interval filter
    interval_filter = sf.expr("creationdate BETWEEN current_timestamp() - INTERVAL 6 MONTHS AND current_timestamp()")

    # Subquery for questions
    q_df = questions.filter(interval_filter) \
        .groupBy("owneruserid") \
        .agg(sf.countDistinct("id").alias("q_total"))
    
    if partitions:
        q_df.repartition(partitions)

    # Subquery for answers
    a_df = answers.filter(interval_filter) \
        .groupBy("owneruserid") \
        .agg(sf.countDistinct("id").alias("a_total"))

    if partitions:
        a_df.repartition(partitions)

    # Subquery for comments
    c_df = comments.filter(interval_filter) \
        .groupBy("userid") \
        .agg(sf.countDistinct("id").alias("c_total"))

    if partitions:
        c_df.repartition(partitions)

    # Join the subqueries with the users dataframe
        # .join(q_df.hint("broadcast"), users["id"] == q_df["owneruserid"], "left_outer") \
        # .join(a_df.hint("broadcast"), users["id"] == a_df["owneruserid"], "left_outer") \
        # .join(c_df.hint("broadcast"), users["id"] == c_df["userid"], "left_outer") \
    return users \
        .join(q_df, users["id"] == q_df["owneruserid"], "left_outer") \
        .join(a_df, users["id"] == a_df["owneruserid"], "left_outer") \
        .join(c_df, users["id"] == c_df["userid"], "left_outer") \
        .select(
            users["id"],
            users["displayname"],
            (sf.coalesce(q_df["q_total"], sf.lit(0)) + 
            sf.coalesce(a_df["a_total"], sf.lit(0)) + 
            sf.coalesce(c_df["c_total"], sf.lit(0))).alias("total")
        ) \
        .orderBy(sf.col("total").desc()) \
        .limit(100) \
        .collect()

@timeit
def q2(spark: SparkSession, users: DataFrame, answers: DataFrame, votes: DataFrame, votesTypes: DataFrame, partitions: int = None) -> List[Row]:
    # votos
    votos_df = votes \
        .join(votesTypes, votes["votetypeid"] == votesTypes["id"]) \
        .filter((votesTypes["name"] == "AcceptedByOriginator") & 
            (votes["creationdate"] >= sf.date_sub(sf.current_date(), 5 * 365))) \
        .select("postid")
    
    # respostas
    respostas_df = answers \
        .filter(answers["id"].isin(votos_df.select("postid").rdd.flatMap(lambda x: x).collect())) \
        .select("owneruserid")

    # utilizadores
    utilizadores_df = users \
        .filter(users["id"].isin(respostas_df.select("owneruserid").rdd.flatMap(lambda x: x).collect())) \
        .withColumn("ano", sf.year(users["creationdate"])) \
        .withColumn("rep", sf.floor(users["reputation"] / 5000) * 5000) \
        .select("id", "ano", "reputation", "rep")

    # years
    current_year = int(spark.sql("SELECT year(current_date())").collect()[0][0])
    years_df = spark.range(2008, current_year + 1).toDF("year")

    # max_reputations
    max_reputations_df = utilizadores_df \
        .join(years_df, utilizadores_df["ano"] == years_df["year"]) \
        .groupBy("year") \
        .agg(sf.max("reputation").cast("int").alias("rep"))

    # buckets
    buckets_df = max_reputations_df \
        .withColumn("reputation_range", sf.expr("sequence(0, rep, 5000)")) \
        .withColumn("reputation_range", sf.explode("reputation_range"))
    
    # Resultado final
    result_df = buckets_df \
        .join(utilizadores_df, (utilizadores_df["ano"] == buckets_df["year"]) & (utilizadores_df["rep"] == buckets_df["reputation_range"]), "left") \
        .groupBy("year", "reputation_range") \
        .agg(sf.count("id").alias("total")) \
        .orderBy("year", "reputation_range")

def main():   
    @timeit
    def w1():
        showPartitionSize("users", users)
        showPartitionSize("questions", questions)
        showPartitionSize("answers", answers)
        showPartitionSize("comments", comments)
        q1(users, questions, answers, comments)
        #print_rows(result)

    @timeit
    def w1p():
        partitions = 5
        u = users.withColumn("salt", sf.rand()).repartition(partitions, "salt")
        q = questions.withColumn("salt", sf.rand()).repartition(partitions, "salt")
        a = answers.withColumn("salt", sf.rand()).repartition(partitions, "salt")
        c = comments.withColumn("salt", sf.rand()).repartition(partitions, "salt")
        
        showPartitionSize("users", u)
        showPartitionSize("questions", q)
        showPartitionSize("answers", a)
        showPartitionSize("comments", c)

        q1(u, q, a, c, partitions)
        # q1(users, questions, answers, comments, partitions)
        #print_rows(result)
    
    
    @timeit
    def w2():
        showPartitionSize("votesTypes", votesTypes)
        showPartitionSize("votes", votes)
        showPartitionSize("answers", answers)
        showPartitionSize("users", users)
        
        q2(spark, users, answers, votes, votesTypes)
        #print_rows(result)

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
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
        # Com adaptive disabled demora o dobro
        # .config("spark.sql.shuffle.partitions", 200) \
        # .config("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024) \
        # .config("spark.executor.memory", "1g") \
        #.config("spark.sql.adaptive.localShuffleReader.enabled", True)

    # google cloud service account credentials file
    spark._jsc.hadoopConfiguration().set(
        "google.cloud.auth.service.account.json.keyfile",
        "/app/credentials.json")

    # data frames
    users = read_parquet_file(spark, "Users")
    questions = read_parquet_file(spark, "Questions")
    answers = read_parquet_file(spark, "Answers")
    comments = read_parquet_file(spark, "Comments")
    votes = read_parquet_file(spark, "Votes")
    votesTypes = read_parquet_file(spark, "VotesTypes")

    locals()[sys.argv[1]]()


if __name__ == "__main__":
    main()