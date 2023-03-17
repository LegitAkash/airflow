from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def my_python_function():
    """
    This is a simple Python function that prints "Hello, Airflow!" to the console.
    """
    print("Hello, Airflow!")


with DAG(
    'my_dag',
    default_args=default_args,
    description='A simple DAG that runs a Python script and logs the output',
    schedule_interval=timedelta(days=1),
) as dag:

    task_run_python_script = PythonOperator(
        task_id='run_python_script',
        python_callable=my_python_function,
        dag=dag,
    )

    # Add a logging task to print the contents of the Airflow log file
    task_print_logs = BashOperator(
        task_id='print_logs',
        bash_command='cat $AIRFLOW_HOME/logs/my_dag/run_python_script/latest.log',
        dag=dag,
    )

    # Set the task dependencies
    task_run_python_script >> task_print_logs

