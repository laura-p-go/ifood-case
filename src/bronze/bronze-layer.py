# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,input_file_name,current_timestamp,to_date,year,month,lit
from pathlib import Path
from functools import reduce
import os

# %%
output_path = "../../data/bronze/yellow_taxi"
Path(output_path).mkdir(parents=True, exist_ok=True)

# %%
landing_path = "../../data/landing/yellow_taxi"


# %%
def create_spark_session() -> SparkSession:
    """
    Cria a SparkSession para execução local.
    """
    spark = (
        SparkSession.builder
        .appName("bronze-layer")
        .master("local[*]")
        .config("spark.hadoop.io.native.lib", "false")
        .config("spark.hadoop.io.native.lib.available", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

def extract_from_landing(spark, landing_path):

    files = [os.path.join(landing_path, f)for f in os.listdir(landing_path)if f.endswith(".parquet")]
    
    return {
        os.path.basename(file): spark.read.parquet(file)
        for file in files
    }

def transform_to_bronze(df):
    """
    Aplica apenas transformações mínimas para a camada Bronze:
    - preserva as colunas originais
    - adiciona colunas técnicas de auditoria
    - adiciona colunas de particionamento derivadas da data de pickup
    """
    df_bronze = (
        df
        .withColumn("source_file", input_file_name())
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", to_date(current_timestamp()))
        .withColumn("pickup_year", year("tpep_pickup_datetime"))
        .withColumn("pickup_month", month("tpep_pickup_datetime"))
    )

    return df_bronze

def normalize_schema(df):
    if "Airport_fee" in df.columns:
        df = df.withColumnRenamed("Airport_fee", "airport_fee")

    expected_columns = {
        "VendorID": "long",
        "tpep_pickup_datetime": "timestamp",
        "tpep_dropoff_datetime": "timestamp",
        "passenger_count": "double",
        "trip_distance": "double",
        "RatecodeID": "double",
        "store_and_fwd_flag": "string",
        "PULocationID": "long",
        "DOLocationID": "long",
        "payment_type": "long",
        "fare_amount": "double",
        "extra": "double",
        "mta_tax": "double",
        "tip_amount": "double",
        "tolls_amount": "double",
        "improvement_surcharge": "double",
        "total_amount": "double",
        "congestion_surcharge": "double",
        "airport_fee": "double",
        "source_file": "string",
        "ingestion_timestamp": "timestamp",
        "ingestion_date": "date",
        "pickup_year": "int",
        "pickup_month": "int",
    }
    # add missing columns
    for col_name, col_type in expected_columns.items():
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast(col_type))
    # cast all columns
    for col_name, col_type in expected_columns.items():
        df = df.withColumn(col_name, col(col_name).cast(col_type))
    # reorder columns
    df = df.select(*expected_columns.keys())
    return df

    
def load_to_bronze(df_bronze, bronze_path: str):
    """
    Salva os dados da Bronze em Parquet,
    particionados por ano e mês da data de pickup.
    """
    (
        df_bronze.write
        .mode("append")
        .partitionBy("pickup_year", "pickup_month")
        .parquet(bronze_path)
    )



# %%
spark = create_spark_session()

# %%
df_dicts = extract_from_landing(spark, landing_path)
for name_file, df in df_dicts.items():
    df_dicts[name_file] = transform_to_bronze(df_dicts[name_file])
    df_dicts[name_file] = normalize_schema(df_dicts[name_file])
    load_to_bronze(df_dicts[name_file], output_path)

# %%
spark.stop()

# %%



