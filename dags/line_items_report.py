from datetime import datetime, timedelta
import kagglehub
from pyspark.sql import SparkSession, functions as sf

from airflow.operators.python import PythonOperator

from airflow.sdk import DAG

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
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["tpc-h pipeline"],
) as dag:
    dag.doc_md = """
        This is a documentation placed anywhere
        """


    def print_context(ds=None, **kwargs):
        """   """
        path = kagglehub.dataset_download("marcogorelli/tpc-h-parquet-s-1")

        # print("Path to dataset files:", path)
        #
        # spark = SparkSession.builder.getOrCreate()
        # df_lineitem = spark.read.parquet(path + "/lineitem.parquet")
        # df_lineitem.printSchema()
        # result = df_lineitem.groupBy(df_lineitem.l_orderkey).agg(
        #     sf.count("*").alias("count"),
        #     sf.sum("l_extendedprice").alias("sum_extendprice"),
        #     sf.mean("l_discount").alias("mean_discount"),
        #     # sf.round("mean_discount", 2),
        #     sf.avg("l_tax").alias("mean_tax"),
        #     sf.avg(sf.datediff("l_receiptdate", "l_shipdate")).alias("delivery_days"),
        #     sf.sum(sf.when(df_lineitem['l_returnflag'] == "A", 1).otherwise(0)).alias("A_return_flags"),
        #     sf.sum(sf.when(df_lineitem['l_returnflag'] == "R", 1).otherwise(0)).alias("R_return_flags"),
        #     sf.sum(sf.when(df_lineitem['l_returnflag'] == "N", 1).otherwise(0)).alias("N_return_flags"),
        # ).sort(df_lineitem.l_orderkey.asc())
        # result.show(5)

        return path


    run_this = PythonOperator(task_id="print_the_context", python_callable=print_context)


    run_this