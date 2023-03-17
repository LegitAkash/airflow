from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args={
    'owner':'Akash',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

def greet(name,age):
    print(f'hello World my name is{name} and my age is{age}')

with DAG(
    default_args=default_args,
    dag_id="our_dag_with_python_operator_v02",
    description='first file using python operator',
    start_date=datetime.today(),
    schedule_interval='@daily'
) as dag:
    task1=PythonOperator(
        task_id='greeting',
        python_callable=greet,
        op_kwargs={"name":"tom", "age":20}
    )
task1