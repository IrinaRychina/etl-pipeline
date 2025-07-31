import os
import kagglehub
from pyspark.sql import SparkSession, functions as sf
import pandas as pd
import clickhouse_connect
from airflow.hooks.base import BaseHook

DATA_PATH = "/opt/airflow/dags/data/tpch"

def transform_customers(**kwargs):
    print("Submitting customers...")

def transform_orders(**kwargs):
    print("Submitting orders...")

def transform_parts(**kwargs):
    print("Submitting parts...")

def transform_suppliers(**kwargs):
    print("Submitting suppliers...")

def transform_lineitems(**kwargs):
    print("Transforming lineitems...")
    spark = SparkSession.builder \
        .appName("line_items_report") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    df_lineitem = spark.read.parquet(DATA_PATH + "/lineitem.parquet")
    df_lineitem.printSchema()
    result = df_lineitem.groupBy(df_lineitem.l_orderkey).agg(
        sf.count("*").alias("count"),
        sf.sum("l_extendedprice").alias("sum_extendprice"),
        sf.mean("l_discount").alias("mean_discount"),
        # sf.round("mean_discount", 2),
        sf.avg("l_tax").alias("mean_tax"),
        sf.avg(sf.datediff("l_receiptdate", "l_shipdate")).alias("delivery_days"),
        sf.sum(sf.when(df_lineitem['l_returnflag'] == "A", 1).otherwise(0)).alias("A_return_flags"),
        sf.sum(sf.when(df_lineitem['l_returnflag'] == "R", 1).otherwise(0)).alias("R_return_flags"),
        sf.sum(sf.when(df_lineitem['l_returnflag'] == "N", 1).otherwise(0)).alias("N_return_flags"),
    ).sort(df_lineitem.l_orderkey.asc())
    result.show(5)
    os.makedirs(DATA_PATH + "/output/line_items_report", exist_ok=True)
    result.write \
        .mode("overwrite") \
        .format("parquet") \
        .save(DATA_PATH + "/output/line_items_report")
    return 'success'

def datamart_customers(**kwargs):
    print("Building datamart for customers")

def datamart_orders(**kwargs):
    print("Building datamart for orders")

def datamart_lineitems(**kwargs):
    print("Building datamart for lineitems")
    df = pd.read_parquet(DATA_PATH + "/output/line_items_report")

    conn = BaseHook.get_connection("clickhouse_default")
    client = clickhouse_connect.get_client(
        host=conn.host,
        port=8123,
        username=conn.login,
        password=conn.password
    )

    create_table_sql = """
           CREATE TABLE IF NOT EXISTS line_items_report (
               l_orderkey UInt32,
               count UInt32,
               sum_extendprice Float64,
               mean_discount Float64,
               mean_tax Float64,
               delivery_days Float64,
               A_return_flags UInt32,
               R_return_flags UInt32,
               N_return_flags UInt32
           ) ENGINE = MergeTree()
           ORDER BY l_orderkey
           """
    client.command(create_table_sql)

    client.insert_df("line_items_report", df)

    print("✅ Data inserted into ClickHouse successfully")

def datamart_parts(**kwargs):
    print("Building datamart for parts")

def datamart_suppliers(**kwargs):
    print("Building datamart for suppliers")

def download_data(**kwargs):
    path = kagglehub.dataset_download("marcogorelli/tpc-h-parquet-s-1")
    os.makedirs(DATA_PATH, exist_ok=True)
    os.system(f"cp -r {path}/* {DATA_PATH}/")

