from pyspark.sql import SparkSession

from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "SalesPipeline") -> SparkSession:
    spark = SparkSession.builder.getOrCreate()
    return spark
