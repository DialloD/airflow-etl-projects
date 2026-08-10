from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd


default_args = {
    "owner": "dounde",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def extract_data():
    data = {
        "product_id": [1, 2, 3, 4],
        "product_name": ["Doliprane", "Amoxicilline", "Ibuprofene", "Paracetamol"],
        "quantity": [10, 20, 5, 15],
        "price": [2.5, 6.0, 4.0, 3.0],
    }

    df = pd.DataFrame(data)

    print("Extracted data:")
    print(df)

    return df.to_dict()


def check_quality(ti):
    data = ti.xcom_pull(task_ids="extract_data")

    df = pd.DataFrame(data)

    if df.empty:
        raise ValueError("Dataset is empty")

    if df["product_id"].isnull().any():
        raise ValueError("product_id contains NULL values")

    if df["quantity"].lt(0).any():
        raise ValueError("Negative quantity detected")

    print("Data quality checks passed")


def transform_data(ti):
    data = ti.xcom_pull(task_ids="extract_data")

    df = pd.DataFrame(data)

    df["total_amount"] = df["quantity"] * df["price"]

    print("Transformed data:")
    print(df)

    return df.to_dict()


def load_data(ti):
    data = ti.xcom_pull(task_ids="transform_data")

    df = pd.DataFrame(data)

    print("Loading data into target database...")
    print(df)

    print(f"{len(df)} rows loaded successfully")


with DAG(
    dag_id="pharma_etl_pipeline",
    description="Simple pharmaceutical ETL pipeline",
    default_args=default_args,
    start_date=datetime(2026, 8, 1),
    schedule="@daily",
    catchup=False,
    tags=["pharma", "etl"],
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    quality = PythonOperator(
        task_id="check_quality",
        python_callable=check_quality,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    success = EmptyOperator(
        task_id="pipeline_success",
    )

    failure = EmptyOperator(
        task_id="pipeline_failure",
        trigger_rule="one_failed",
    )

    extract >> quality >> transform >> load >> success

    [extract, quality, transform, load] >> failure