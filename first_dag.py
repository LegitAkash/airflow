from datetime import datetime,timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args={
    'owner':"akash",
    "retries":5,
    "retry_delay":timedelta(minutes=2)
}

with DAG(
        dag_id='first_dag',
        default_args=default_args,
        description='this is the First Dag that i want to exicute', 
        start_date=datetime(2023,3,16,13),
        schedule_interval='@daily'
) as dag:
    task1=BashOperator(
        task_id='first_task',
        bash_command="echo hello word this is the first task !"
    )
    task2= BashOperator(
        task_id='second_task',
        bash_command='echo hey,I am task2 and will be running after task 1'
    )
    task3=BashOperator(
        task_id='third_task',
        bash_command='echo this is the task3 and will be running after task2'
    )
# task1.set_downstream(task2)
# task1.set_downstream(task3)
task1>>[task2,task3]