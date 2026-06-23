# %%
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    year,
    month,
    hour,
    to_date,
    unix_timestamp
)
from pathlib import Path

# %%
BRONZE_PATH = "../../data/bronze/yellow_taxi"
SILVER_PATH = "../../data/silver/yellow_taxi"

Path(SILVER_PATH).mkdir(parents=True, exist_ok=True)

# %%

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("silver-yellow-taxi")
        .master("local[*]")
        .config("spark.hadoop.io.native.lib", "false")
        .config("spark.hadoop.io.native.lib.available", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def extract_from_bronze(spark: SparkSession, bronze_path: str) -> DataFrame:
    return spark.read.parquet(bronze_path)


def transform_to_silver(df: DataFrame) -> DataFrame:
    """
    Curates Yellow Taxi bronze data into a clean Silver dataset.
    """

    df_silver = (
        df
        .select(
            col("VendorID").cast("int").alias("VendorID"),
            col("passenger_count").cast("int").alias("passenger_count"),
            col("trip_distance").cast("double").alias("trip_distance"),
            col("payment_type").cast("int").alias("payment_type"),
            col("fare_amount").cast("double").alias("fare_amount"),
            col("tip_amount").cast("double").alias("tip_amount"),
            col("tolls_amount").cast("double").alias("tolls_amount"),
            col("total_amount").cast("double").alias("total_amount"),
            col("tpep_pickup_datetime").cast("timestamp").alias("tpep_pickup_datetime"),
            col("tpep_dropoff_datetime").cast("timestamp").alias("tpep_dropoff_datetime"),
            col("source_file"),
            col("ingestion_timestamp")
        )
        # quality filters
        .filter(col("tpep_pickup_datetime").isNotNull())
        .filter(col("tpep_dropoff_datetime").isNotNull())
        .filter(col("total_amount").isNotNull())
        .filter(col("passenger_count").isNotNull())
        .filter(col("passenger_count") >= 0)
        .filter(col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime"))
        # derived columns
        .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
        .withColumn("pickup_year", year(col("tpep_pickup_datetime")))
        .withColumn("pickup_month", month(col("tpep_pickup_datetime")))
        .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
        .withColumn(
            "trip_duration_minutes",
            (
                unix_timestamp(col("tpep_dropoff_datetime")) -
                unix_timestamp(col("tpep_pickup_datetime"))
            ) / 60.0
        )
    )

    return df_silver


def load_to_silver(df: DataFrame, silver_path: str) -> None:
    (
        df.write
        .mode("overwrite")
        .partitionBy("pickup_year", "pickup_month")
        .parquet(silver_path)
    )

# %%
spark = create_spark_session()

df_bronze = extract_from_bronze(spark, BRONZE_PATH)
df_silver = transform_to_silver(df_bronze)
load_to_silver(df_silver, SILVER_PATH)

spark.stop()


