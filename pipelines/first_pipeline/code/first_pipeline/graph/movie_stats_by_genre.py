from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from first_pipeline.config.ConfigStore import *
from first_pipeline.udfs.UDFs import *

def movie_stats_by_genre(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("genres"))

    return df1.agg(
        first(col("movie_title")).alias("movie_title"), 
        first(col("production_date")).alias("production_date"), 
        first(col("runtime_minutes")).alias("runtime_minutes"), 
        first(col("director_name")).alias("director_name"), 
        first(col("director_professions")).alias("director_professions"), 
        first(col("director_birthYear")).alias("director_birthYear"), 
        first(col("director_deathYear")).alias("director_deathYear"), 
        first(col("movie_averageRating")).alias("movie_averageRating"), 
        first(col("movie_numerOfVotes")).alias("movie_numerOfVotes"), 
        first(col("approval_Index")).alias("approval_Index"), 
        first(col("`Production budget $`")).alias("Production budget $"), 
        first(col("`Domestic gross $`")).alias("Domestic gross $"), 
        first(col("`Worldwide gross $`")).alias("Worldwide gross $")
    )
