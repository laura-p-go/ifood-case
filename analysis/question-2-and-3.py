# %%
import duckdb

# %%
GOLD_PATH = "../data/gold"

con = duckdb.connect()

# Views apontando para os arquivos parquet da camada gold
con.execute(f"""
    CREATE OR REPLACE VIEW gold_monthly_metrics AS
    SELECT *
    FROM read_parquet('{GOLD_PATH}/yellow_taxi_monthly_metrics/*.parquet');
""")

con.execute(f"""
    CREATE OR REPLACE VIEW gold_may_hourly_passenger_metrics AS
    SELECT *
    FROM read_parquet('{GOLD_PATH}/yellow_taxi_may_hourly_passenger_metrics/*.parquet');
""")

# %%
print("=" * 80)
print("PERGUNTA 1")
print("Qual a média de valor total (total_amount) recebido em um mês")
print("considerando todos os yellow táxis da frota?")
print("=" * 80)

query_1 = """
    SELECT
        pickup_year,
        pickup_month,
        avg_total_amount
    FROM gold_monthly_metrics
    ORDER BY pickup_year, pickup_month;
"""

result_1 = con.execute(query_1).fetchdf()
print(result_1)

# %%
print("=" * 80)
print("PERGUNTA 2")
print("Qual a média de passageiros (passenger_count) por cada hora do dia")
print("que pegaram táxi no mês de maio considerando todos os táxis da frota?")
print("=" * 80)

query_2 = """
    SELECT
        pickup_hour,
        avg_passenger_count
    FROM gold_may_hourly_passenger_metrics
    ORDER BY pickup_hour;
"""

result_2 = con.execute(query_2).fetchdf()
print(result_2)

con.close()


