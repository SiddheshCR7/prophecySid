from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from first_pipeline.config.ConfigStore import *
from first_pipeline.udfs.UDFs import *
from prophecy.utils import *
from first_pipeline.graph import *

def pipeline(spark: SparkSession) -> None:
    df_movie = movie(spark)
    df_movie_stats_by_genre = movie_stats_by_genre(spark, df_movie)
    df_by_worldwide_gross_desc_nulls_first = by_worldwide_gross_desc_nulls_first(spark, df_movie_stats_by_genre)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/first_pipeline")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/first_pipeline", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/first_pipeline")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
