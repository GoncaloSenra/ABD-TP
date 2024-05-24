from typing import List
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import count, udf, col, date_trunc, avg, when, rand
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import StringType, TimestampType
import time
from functools import wraps
import sys


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
    return spark.read.parquet(f"gs://parquet-data-bucket/{name}.parquet")

def expensiveFunc(s: str):
    for _ in range(20000):
        pass
    return s

expensiveFuncUdf = udf(lambda x: expensiveFunc(x), StringType())

@timeit
def q3(tags, questionstags, answers, min_tag_count):
    subquery1 = tags \
        .join(questionstags, questionstags['TagId'] == tags['Id']) \
        .groupBy(tags['Id'], tags['TagName']) \
        .agg(count("*").alias("tag_count")) \
        .filter(col("tag_count") > min_tag_count) \
        .select(tags['Id'], tags['TagName'])
    
    subquery2 = subquery1 \
        .join(questionstags.alias("qt"), questionstags['TagId'] == subquery1['Id']) \
        .join(answers.alias("a"), col("a.ParentId") == col("qt.QuestionId"), "left") \
        .groupBy(subquery1['TagName'], col("qt.QuestionId")) \
        .agg(count("*").alias("total")) \
        .select(subquery1["TagName"], col("qt.QuestionId"), col("total"))

    result = subquery2 \
        .groupBy(subquery2['TagName']) \
        .agg(spark_round(avg(subquery2["total"]), 3).alias("avg_total"), count("*").alias("count")) \
        .orderBy(col("avg_total").desc(), col("count").desc(), subquery2["TagName"]) \
        .collect()
        
    return result

@timeit
def q3_partitions(tags, questionstags, answers, min_tag_count, partitions: int = None):
    
    subquery1 = tags \
        .join(questionstags, questionstags['TagId'] == tags['Id']) \
        .groupBy(tags['Id'], tags['TagName']) \
        .agg(count("*").alias("tag_count")) \
        .filter(col("tag_count") > min_tag_count) \
        .select(tags['Id'], tags['TagName'])

    if partitions:
        subquery1.repartition(partitions)
    
    subquery2 = subquery1 \
        .join(questionstags.alias("qt"), questionstags['TagId'] == subquery1['Id']) \
        .join(answers.alias("a"), col("a.ParentId") == col("qt.QuestionId"), "left") \
        .groupBy(subquery1['TagName'], col("qt.QuestionId")) \
        .agg(count("*").alias("total")) \
        .select(subquery1["TagName"], col("qt.QuestionId"), col("total"))
    
    if partitions:
        subquery2.repartition(partitions)

    # Main query: Aggregate by tagname, compute average and count, and order the result
    result = subquery2 \
        .groupBy(subquery2['TagName']) \
        .agg(spark_round(avg(subquery2["total"]), 3).alias("avg_total"), count("*").alias("count")) \
        .orderBy(col("avg_total").desc(), col("count").desc(), subquery2["TagName"]) \
        .collect()
        
    return result


def q3_v2(tags, questionstags, answers, min_tag_count):
    # Primeira parte da consulta para calcular 'total' por tag
    subquery1 = tags \
        .join(questionstags.alias("qt"), questionstags['TagId'] == tags['Id']) \
        .join(answers.alias("a"), col("a.ParentId") == col("qt.QuestionId"), "left") \
        .groupBy(tags['TagName'], col("qt.QuestionId")) \
        .agg(count("*").alias("total")) \
        .select(tags["TagName"], col("qt.QuestionId"), col("total"))

    # Segunda parte da consulta para calcular a média e contar
    result = subquery1 \
        .groupBy(subquery1['TagName']) \
        .agg(spark_round(avg(subquery1["total"]), 3).alias("avg_total"), count("*").alias("count")) \
        .filter(col("count") > min_tag_count) \
        .orderBy(col("avg_total").desc(), col("count").desc(), subquery1["TagName"]) \
        .collect()
        
    return result

@timeit
def q4(badges: DataFrame, tagbased: bool, name: List[str], class_n: List[int], userid: int) -> List[Row]:
    result = badges \
        .where(~col("TagBased") & \
           ~col("Name").isin(name) & \
           col("Class").isin(class_n) & \
           (col("UserId") != -1)) \
        .select(date_trunc("minute", "Date").alias("date_bin")) \
        .groupBy("date_bin") \
        .count() \
        .orderBy("date_bin")\
        .collect()
    
    return result

@timeit
def q4_optimized(badges, tagbased, name, class_n, userid):
    filtered_badges = badges \
        .filter(~col("TagBased")) \
        .filter(~col("Name").isin(name)) \
        .filter(col("Class").isin(class_n)) \
        .filter(col("UserId") != -1)

    result = filtered_badges  \
        .withColumn("date_bin", date_trunc("minute", col("Date")).cast(TimestampType())) \
        .groupBy("date_bin") \
        .agg({"*": "count"}) \
        .orderBy("date_bin") \
        .collect()
    
    return result

@timeit
def q4_partitioned_date_bin(badges: DataFrame, tagbased: bool, name: List[str], class_n: List[int], userid: int) -> List[Row]:
    
    badges = badges.withColumn("date_bin", date_trunc("minute", "Date"))

    result = badges \
        .where(~col("TagBased") & \
           ~col("Name").isin(name) & \
           col("Class").isin(class_n) & \
           (col("UserId") != -1)) \
        .groupBy("date_bin") \
        .count() \
        .orderBy("date_bin")
    
    return result

def main():

    @timeit
    def run_q4():
        q4(badges, False, ['Analytical', 'Census', 'Documentation Beta', 'Documentation Pioneer', 'Documentation User', 'Reversal', 'Tumbleweed'], [1, 2, 3], -1)

    @timeit
    def run_q4_optimized():
        badges.repartition(3)

        # Adicionar coluna 'salt' com valores aleatórios
        badges_salt = badges.withColumn('salt', rand())
    
        # Reparticionar novamente usando a coluna 'salt' em 8 partições
        badges_salt = badges_salt.repartition(3, 'salt')

        showPartitionSize("badges", badges_salt)
        q4_optimized(badges_salt, False, ['Analytical', 'Census', 'Documentation Beta', 'Documentation Pioneer', 'Documentation User', 'Reversal', 'Tumbleweed'], [1, 2, 3], -1)

    @timeit
    def explain_q4():
        result = q4(badges, False, ['Analytical', 'Census', 'Documentation Beta', 'Documentation Pioneer', 'Documentation User', 'Reversal', 'Tumbleweed'], [1, 2, 3], -1)
        result.explain(mode="extended")

    @timeit
    def print_q4_results():
        result = q4(badges, False, ['Analytical', 'Census', 'Documentation Beta', 'Documentation Pioneer', 'Documentation User', 'Reversal', 'Tumbleweed'], [1, 2, 3], -1)
        result.show()

    @timeit
    def run_q3():
        q3(tags, questionstags, answers, 10)
    
    @timeit
    def explain_q3():
        result = q3(tags, questionstags, answers, 10)
        result.explain(mode="extended")

    @timeit
    def run_q3_partitions():
        partitions = 10

        tags.repartition(10)
        questionstags.repartition(10)
        answers.repartition(10)
        
        # Adicionar coluna 'salt' com valores aleatórios
        #tags_salt = tags.withColumn('salt', rand())
        #tags_salt = tags_salt.repartition(10, 'salt')
        #showPartitionSize("tags", tags_salt)

        #questionstags_salt = questionstags.withColumn('salt', rand())
        #questionstags_salt = questionstags_salt.repartition(10, 'salt')
        #showPartitionSize("questionstags", questionstags_salt)

        #answers_salt = answers.withColumn('salt', rand())
        #answers_salt = answers_salt.repartition(10, 'salt')
        #showPartitionSize("answers", answers_salt)
        
        showPartitionSize("tags", tags)
        showPartitionSize("questionstags", questionstags)
        showPartitionSize("answers", answers)

        q3_partitions(tags, questionstags, answers, 10, partitions)
    
    #@timeit
    #def run_q3_v2():
        #q3_v2(tags, questionstags, answers, 10)
        
    @timeit
    def print_q3_results():
        result = q3(tags, questionstags, answers, 10) 
        result.show()

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
        .config("spark.executor.memory", "1g") \
        .getOrCreate()
        

    spark._jsc.hadoopConfiguration().set(
        "google.cloud.auth.service.account.json.keyfile",
        "/app/abd-2024-419722-d91351ebe666.json")

    # data frames
    badges = read_parquet_file(spark, "Badges")
    tags = read_parquet_file(spark, "Tags")
    questionstags = read_parquet_file(spark, "QuestionsTags")
    #questions = read_parquet_file(spark, "Questions")
    answers = read_parquet_file(spark, "Answers")

    locals()[sys.argv[1]]()

if __name__ == '__main__':
    main()