#importações necessarias
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

#argumentos para a dag
default_args = {
    'owner': 'airflow',  #proprietario DAG
    'depends_on_past': False,  #não depende de execuções anteriores
    'retries': 1,  #número de tentativas em caso de falha
    'retry_delay' : timedelta(minutes=1) #caso demore mais que 1 minuto, considera falha
}

# Definição da segunda DAG
with DAG(
    dag_id='primeiraDAG',  #id da DAG
    default_args=default_args,  #argumentos padrão
    description='Verifica se o arquivo arquivo.txt existe e o cria se necessário',  #descrição da DAG
    schedule_interval=None,  #não possui agendamento (executa manualmente)
    start_date=datetime(2025, 1, 1),  #data de início da DAG
    catchup=False,  #não executa execuções pendentes
) as dag:

    #task 1 verifica se o arquivo existe
    check_file = BashOperator(
        task_id='check_file_existence',  #id task
        bash_command='if [ -f C:/Users/Lucas/Desktop/arquivo.txt ]; then echo "Arquivo existe"; else echo "Arquivo não encontrado"; fi',  #comando para verificar o arquivo
    )

    #task 2 cria um arquivo caso ele nao exista
    create_file = BashOperator(
        task_id='create_file_if_not_exists',  #id task
        bash_command='if [ ! -f C:/Users/Lucas/Desktop/arquivo.txt ]; then touch /caminho/para/arquivo.txt && echo "Arquivo criado"; fi',  #comando para criar o arquivo se necessario
    )

    #dependencia das tasks
    check_file >> create_file