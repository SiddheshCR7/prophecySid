from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from first_pipeline.config.ConfigStore import *
from first_pipeline.udfs.UDFs import *

def movie(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("movie_title", StringType(), True), StructField("production_date", DateType(), True), StructField("genres", StringType(), True), StructField("runtime_minutes", DoubleType(), True), StructField("director_name", StringType(), True), StructField("director_professions", StringType(), True), StructField("director_birthYear", StringType(), True), StructField("director_deathYear", StringType(), True), StructField("movie_averageRating", DoubleType(), True), StructField("movie_numerOfVotes", DoubleType(), True), StructField("approval_Index", DoubleType(), True), StructField("Production budget $", IntegerType(), True), StructField("Domestic gross $", IntegerType(), True), StructField("Worldwide gross $", LongType(), True)
        ])
        )\
        .option("header", True)\
        .option("inferSchema", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/tables/siddhesh/movie_statistic_dataset.csv")
