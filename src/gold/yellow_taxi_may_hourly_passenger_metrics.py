# %%
from pyspark.sql.functions import col, avg, count
from pyspark.sql import SparkSession
from pathlib import Path

# %%
SILVER_PATH = "../../data/silver/yellow_taxi"
GOLD_MONTHLY_PATH = "../../data/gold/yellow_taxi_may_hourly_passenger_metrics"

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

def build_gold_may_hourly_passenger_metrics(df_silver):
    return (
        df_silver
        .filter(col("pickup_month") == 5)
        .groupBy("pickup_hour")
        .agg(
            count("*").alias("trip_count"),
            avg("passenger_count").alias("avg_passenger_count")
        )
        .orderBy("pickup_hour")
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
df_hourly_may = build_gold_may_hourly_passenger_metrics(df_silver)
load_gold(df_hourly_may, GOLD_MONTHLY_PATH)

spark.stop()

# %%


# %%


# %%


# %%


# %%



