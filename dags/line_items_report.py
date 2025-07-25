from datetime import datetime, timedelta
import os

from airflow.operators.python import PythonOperator
from airflow.sdk import DAG

DATA_PATH = "/opt/airflow/dags/data/tpch"

with DAG(
        "line_items_report",
        # These args will get passed on to each operator
        # You can override them on a per-task basis during operator initialization
        default_args={
            "depends_on_past": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        description="tpc-h pipeline",
        schedule=timedelta(days=1),
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["tpc-h pipeline"],
) as dag:
    dag.doc_md = """
        This is a documentation placed anywhere
        """


    def download_data(**kwargs):
        import kagglehub
        path = kagglehub.dataset_download("marcogorelli/tpc-h-parquet-s-1")
        os.makedirs(DATA_PATH, exist_ok=True)
        os.system(f"cp -r {path}/* {DATA_PATH}/")


    def process_data(**kwargs):
        """   """
        from pyspark.sql import SparkSession, functions as sf

        print("Path to dataset files:", DATA_PATH)

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


    def save_data(**kwargs):
        import pandas as pd
        import clickhouse_connect
        from airflow.hooks.base import BaseHook

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


    download_data = PythonOperator(task_id="download_data", python_callable=download_data)
    process_data = PythonOperator(task_id="process_data", python_callable=process_data)
    save_data = PythonOperator(task_id="save_data", python_callable=save_data)

    download_data >> process_data >> save_data
