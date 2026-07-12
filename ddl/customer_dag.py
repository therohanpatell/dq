from airflow import DAG
from airflow.operators.python import PythonOperator

def validate():
    print("Validating customer_id")

with DAG(
    dag_id="customer_pipeline"
):
    validate_task = PythonOperator(
        task_id="validate_customer",
        python_callable=validate
    )
