from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException

default_args = {
    "start_date": days_ago(0),
    "retries": 1,
}

with DAG(
    'bindu_madhuri'
    dag_id="run_sales_transform_sql_gcs",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Run transformation on tds_sales.sales_target using SQL from GCS",
) as dag:

    BUCKET_NAME = "us-central1-madhuri-2cca0acc-bucket"
    SQL_OBJECT_NAME = "dags/sales.sql"

    def load_sql(**kwargs):
        try:
            hook = GCSHook(gcp_conn_id="google_cloud_default")
            sql_bytes = hook.download(bucket_name=BUCKET_NAME, object_name=SQL_OBJECT_NAME)
            sql_text = sql_bytes.decode("utf-8")
            # push into XCom
            kwargs["ti"].xcom_push(key="sql_query", value=sql_text)
        except Exception as e:
            raise AirflowFailException(f"Failed to read SQL from GCS: {e}")

    load_sql_task = PythonOperator(
        task_id="load_sql_from_gcs",
        python_callable=load_sql,
        provide_context=True,
    )

    run_transform_sql = BigQueryInsertJobOperator(
        task_id="run_sql_transform",
        configuration={
            "query": {
                "query": "{{ ti.xcom_pull(key='sql_query', task_ids='load_sql_from_gcs') }}",
                "useLegacySql": False,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default",
    )

    load_sql_task >> run_transform_sql
