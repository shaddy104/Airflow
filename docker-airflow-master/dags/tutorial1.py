from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner' : 'coder_shadrach',
    'retries' : 5,
    'retry_delay': timedelta(minutes=2)
}


with DAG(

    dag_id = 'our_first_dag_v2',
    default_args = default_args,
    description = 'This is our first dag that we write',
    start_date = datetime(2022,8,1),
    schedule_interval = '@daily'
    
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo hello world, this is the first task!'
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo hey, I am task2 and will be running after task1'
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command='echo hey, I am task 3 and will be running after task1'
    )

    #Task dependency method 1
    #task1.set_downstream(task2)
    #task1.set_downstream(task3)

    #Task dependency method 2
    #task1 >> task2
    #task1 >> task3

    #Task dependency method 3
    task1 >> [task2,task3]
