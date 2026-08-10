from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "dounde",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def extract_data():
    import pandas as pd
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
    import pandas as pd
    data = ti.xcom_pull(task_ids="extract_data")

    df = pd.DataFrame(data)

    if df.empty:
        raise ValueError("Dataset is empty")

    if df["product_id"].isnull().any():
        raise ValueError("product_id contains NULL values")

    if df["quantity"].lt(0).any():
        raise ValueError("Negative quantity detected")

    # Failure test
    # raise ValueError("Simulated data quality failure")


def transform_data(ti):
    import pandas as pd
    data = ti.xcom_pull(task_ids="extract_data")

    df = pd.DataFrame(data)

    df["total_amount"] = df["quantity"] * df["price"]

    print("Transformed data:")
    print(df)

    return df.to_dict()


def load_data(ti):
    import pandas as pd
    import urllib.request
    import base64
    import json

    data = ti.xcom_pull(task_ids="transform_data")
    df = pd.DataFrame(data)

    host = "clickhouse.clickhouse.svc.cluster.local"
    user = "airflow"
    password = "Airflow123!"

    auth = base64.b64encode(
        f"{user}:{password}".encode()
    ).decode()

    create_sql = """
    CREATE TABLE IF NOT EXISTS default.pharma_sales
    (
        sale_id UInt64,
        product_name String,
        quantity UInt32,
        price Float64,
        total_amount Float64,
        version UInt64
    )
    ENGINE = ReplacingMergeTree(version)
    ORDER BY sale_id
    """

    req = urllib.request.Request(
        f"http://{host}:8123/",
        data=create_sql.encode(),
        method="POST",
    )
    req.add_header("Authorization", f"Basic {auth}")
    urllib.request.urlopen(req).read()

    rows = []

    for i, row in df.iterrows():
        rows.append({
            "sale_id": int(row["product_id"]),
            "product_name": str(row["product_name"]),
            "quantity": int(row["quantity"]),
            "price": float(row["price"]),
            "total_amount": float(row["total_amount"]),
            "version": 1,
        })

    payload = "\n".join(json.dumps(row) for row in rows)

    insert_url = (
        f"http://{host}:8123/"
        "?query=INSERT%20INTO%20default.pharma_sales%20FORMAT%20JSONEachRow"
    )

    req = urllib.request.Request(
        insert_url,
        data=payload.encode(),
        method="POST",
    )
    req.add_header("Authorization", f"Basic {auth}")
    urllib.request.urlopen(req).read()

    print(f"{len(rows)} rows loaded into ClickHouse")


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