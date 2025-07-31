from datetime import datetime, timedelta
import os

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from tasks import entity_tasks

transform_funcs = {
    "customers": entity_tasks.transform_customers,
    "lineitems": entity_tasks.transform_lineitems,
    "orders": entity_tasks.transform_orders,
    "parts": entity_tasks.transform_parts,
    "suppliers": entity_tasks.transform_suppliers,
}

datamart_funcs = {
    "customers": entity_tasks.datamart_customers,
    "lineitems": entity_tasks.datamart_lineitems,
    "orders": entity_tasks.datamart_orders,
    "parts": entity_tasks.datamart_parts,
    "suppliers": entity_tasks.datamart_suppliers,
}

with DAG(
        "tpch_report",
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

    start = EmptyOperator(task_id="start")
    extract = PythonOperator(task_id="extract", python_callable=entity_tasks.download_data)
    end = EmptyOperator(task_id="end")

    entities = ["customers", "lineitems", "orders", "parts", "suppliers"]

    for entity in entities:
        transform = PythonOperator(
            task_id=f"transform_{entity}",
            python_callable=transform_funcs[entity]
        )

        datamart = PythonOperator(
            task_id=f"datamart_{entity}",
            python_callable=datamart_funcs[entity]
        )

        start >> extract >> transform >> datamart >> end
