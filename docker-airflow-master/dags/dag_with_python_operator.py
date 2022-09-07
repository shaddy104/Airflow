from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'Shadrach',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def greet(**context):
    first_name = context['task_instance'].xcom_pull(task_ids='get_name', key='first_name')
    last_name = context['task_instance'].xcom_pull(task_ids='get_name', key='last_name')
    age = context['task_instance'].xcom_pull(task_ids='get_age', key='age')
    print(f'Hello, I am {first_name} {last_name} '
    f'and I am {age} years old')

def get_name(**context):
    context['task_instance'].xcom_push(key='first_name', value='Jerry')
    context['task_instance'].xcom_push(key='last_name', value='Fridman')

def get_age(**context):
    context['task_instance'].xcom_push(key='age', value=19)

with DAG(
    default_args = default_args,
    dag_id = 'Dag_with_python_operator_v1',
    description = 'Dag with python operator learning',
    start_date = datetime(2022,8,25),
    schedule_interval = '@daily'

) as dag:
    task1 = PythonOperator(
        task_id = 'greet',
        python_callable = greet,
        #op_kwargs= {'age': 27}
        provide_context=True
    )

    task2 = PythonOperator(
        task_id = 'get_name',
        provide_context=True,
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id = 'get_age',
        provide_context=True,
        python_callable=get_age
    )

    task2 >> task3 >> task1