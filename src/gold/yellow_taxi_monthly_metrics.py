# %%
from pyspark.sql.functions import col, avg, sum as _sum, count
from pyspark.sql import SparkSession
from pathlib import Path

# %%
SILVER_PATH = "../../data/silver/yellow_taxi"
GOLD_MONTHLY_PATH = "../../data/gold/yellow_taxi_monthly_metrics"

Path(GOLD_MONTHLY_PATH).mkdir(parents=True, exist_ok=True)

# %%
def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("gold-yellow-taxi")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def extract_from_silver(spark: SparkSession, silver_path: str) -> DataFrame:
    return spark.read.parquet(silver_path)

def build_gold_monthly_metrics(df_silver):
    return (
        df_silver
        .groupBy("pickup_year", "pickup_month")
        .agg(
            count("*").alias("trip_count"),
            avg("total_amount").alias("avg_total_amount"),
            _sum("total_amount").alias("sum_total_amount"),
            avg("passenger_count").alias("avg_passenger_count")
        )
        .orderBy("pickup_year", "pickup_month")
    )

def load_gold(df: DataFrame, output_path: str) -> None:
    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )



# %%
spark = create_spark_session()

df_silver = extract_from_silver(spark, SILVER_PATH)

df_monthly = build_gold_monthly_metrics(df_silver)

load_gold(df_monthly, GOLD_MONTHLY_PATH)

spark.stop()

# %%



