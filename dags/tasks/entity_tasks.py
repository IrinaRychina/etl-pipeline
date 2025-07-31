import os
import kagglehub
from pyspark.sql import SparkSession, functions as sf
import pandas as pd
import clickhouse_connect
from airflow.hooks.base import BaseHook

DATA_PATH = "/opt/airflow/dags/data/tpch"

def transform_customers(**kwargs):
    print("Transforming customers...")
    spark = SparkSession.builder \
        .appName("customers_report") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    df_region = spark.read.parquet(DATA_PATH + "/region.parquet")
    df_customer = spark.read.parquet(DATA_PATH + "/customer.parquet")
    df_nation = spark.read.parquet(DATA_PATH + "/nation.parquet")

    result = df_customer.join(df_nation, df_nation.n_nationkey == df_customer.c_nationkey) \
        .join(df_region, df_nation.n_regionkey == df_region.r_regionkey) \
        .groupBy([df_region.r_name, df_nation.n_name, df_customer.c_mktsegment, ]) \
        .agg(
        sf.count("*").alias("unique_customers_count"),
        sf.avg("c_acctbal").alias("avg_acctbal"),
        sf.mean("c_acctbal").alias("mean_acctbal"),
        sf.min("c_acctbal").alias("min_acctbal"),
        sf.max("c_acctbal").alias("max_acctbal"),
    ).sort([df_nation.n_name, df_customer.c_mktsegment])

    result.show(5)
    os.makedirs(DATA_PATH + "/output/customers_report", exist_ok=True)
    result.write \
        .mode("overwrite") \
        .format("parquet") \
        .save(DATA_PATH + "/output/customers_report")
    return 'success'

def transform_orders(**kwargs):
    print("Transforming orders...")
    spark = SparkSession.builder \
        .appName("orders_report") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    df_orders = spark.read.parquet(DATA_PATH + "/orders.parquet")
    df_customer = spark.read.parquet(DATA_PATH + "/customer.parquet")
    df_nation = spark.read.parquet(DATA_PATH + "/nation.parquet")

    df_orders_with_month = df_orders.withColumn("o_month", sf.date_format('o_orderdate', 'yyyy-MM'))
    result = df_orders_with_month.join(df_customer, df_orders_with_month.o_custkey == df_customer.c_custkey) \
        .join(df_nation, df_nation.n_nationkey == df_customer.c_nationkey) \
        .groupBy([df_orders_with_month.o_month, df_nation.n_name, df_orders_with_month.o_orderpriority, ]) \
        .agg(
        sf.count("*").alias("orders_count"),
        sf.avg("o_totalprice").alias("avg_order_price"),
        sf.sum("o_totalprice").alias("sum_order_price"),
        sf.min("o_totalprice").alias("min_order_price"),
        sf.max("o_totalprice").alias("max_order_price"),
        sf.sum(sf.when(df_orders_with_month['o_orderstatus'] == 'F', 1).otherwise(0)).alias("f_order_status"),
        sf.sum(sf.when(df_orders_with_month['o_orderstatus'] == 'O', 1).otherwise(0)).alias("o_order_status"),
        sf.sum(sf.when(df_orders_with_month['o_orderstatus'] == 'P', 1).otherwise(0)).alias("p_order_status"),
    ).sort([df_nation.n_name, df_orders_with_month.o_orderpriority])
    result.show(5)
    os.makedirs(DATA_PATH + "/output/orders_report", exist_ok=True)
    result.write \
        .mode("overwrite") \
        .format("parquet") \
        .save(DATA_PATH + "/output/orders_report")
    return 'success'

def transform_parts(**kwargs):
    print("Transforming parts...")
    spark = SparkSession.builder \
        .appName("parts_report") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    df_partsupp = spark.read.parquet(DATA_PATH + "/partsupp.parquet")
    df_part = spark.read.parquet(DATA_PATH + "/part.parquet")
    df_nation = spark.read.parquet(DATA_PATH + "/nation.parquet")
    df_supplier = spark.read.parquet(DATA_PATH + "/supplier.parquet")

    result = df_part.join(df_partsupp, df_part.p_partkey == df_partsupp.ps_partkey) \
        .join(df_supplier, df_supplier.s_suppkey == df_partsupp.ps_suppkey) \
        .join(df_nation, df_supplier.s_nationkey == df_nation.n_nationkey) \
        .groupBy([df_nation.n_name, df_part.p_type, df_part.p_container]) \
        .agg(
        sf.countDistinct("ps_partkey").alias("parts_count"),
        sf.avg("p_retailprice").alias("avg_retailprice"),
        sf.sum("p_size").alias("size"),
        sf.mean("p_retailprice").alias("mean_retailprice"),
        sf.min("p_retailprice").alias("min_retailprice"),
        sf.max("p_retailprice").alias("max_retailprice"),
        sf.avg("ps_supplycost").alias("avg_supplycost"),
        sf.mean("ps_supplycost").alias("mean_supplycost"),
        sf.min("ps_supplycost").alias("min_supplycost"),
        sf.max("ps_supplycost").alias("max_supplycost"),
    ).sort([df_nation.n_name, df_part.p_type, df_part.p_container])

    result.show(5)
    os.makedirs(DATA_PATH + "/output/parts_report", exist_ok=True)
    result.write \
        .mode("overwrite") \
        .format("parquet") \
        .save(DATA_PATH + "/output/parts_report")
    return 'success'

def transform_suppliers(**kwargs):
    print("Transforming suppliers...")
    spark = SparkSession.builder \
        .appName("suppliers_report") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    df_region = spark.read.parquet(DATA_PATH + "/region.parquet")
    df_supplier = spark.read.parquet(DATA_PATH + "/supplier.parquet")
    df_nation = spark.read.parquet(DATA_PATH + "/nation.parquet")

    result = df_supplier.join(df_nation, df_nation.n_nationkey == df_supplier.s_nationkey) \
        .join(df_region, df_nation.n_regionkey == df_region.r_regionkey) \
        .groupBy([df_region.r_name, df_nation.n_name]) \
        .agg(
        sf.count("*").alias("unique_supplers_count"),
        sf.avg("s_acctbal").alias("avg_acctbal"),
        sf.mean("s_acctbal").alias("mean_acctbal"),
        sf.min("s_acctbal").alias("min_acctbal"),
        sf.max("s_acctbal").alias("max_acctbal"),
    ).sort([df_region.r_name, df_nation.n_name])

    result.show(5)
    os.makedirs(DATA_PATH + "/output/suppliers_report", exist_ok=True)
    result.write \
        .mode("overwrite") \
        .format("parquet") \
        .save(DATA_PATH + "/output/suppliers_report")
    return 'success'

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

