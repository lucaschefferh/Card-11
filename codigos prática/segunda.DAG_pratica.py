from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

#função 1
def print_hello():
    print("Hello, Airflow!")

#função 2
def check_even_or_odd(number: int):
    if number % 2 == 0:
        print(f"O número {number} é par.")
    else:
        print(f"O número {number} é ímpar.")

#argumentos para a DAG
default_args = {
    'owner': 'airflow', #proprietario
    'depends_on_past': False, #não depende de execuções anteriores
    'retries': 1, #tentativas
}

#DAG
with DAG(
    dag_id='python_operator_example',
    default_args=default_args,
    description='Exemplo de DAG utilizando PythonOperator',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    #task 1
    task_hello = PythonOperator(
        task_id='print_hello_task', #id task
        python_callable=print_hello,  #chamada da funcao
    )

    #task 2
    task_check_number = PythonOperator(
        task_id='check_even_or_odd_task', #id task
        python_callable=check_even_or_odd,  #função a ser executada
        op_kwargs={'number': 7},  #passa o número como argumento para a função
    )

    #dependencias 
    task_hello >> task_check_number