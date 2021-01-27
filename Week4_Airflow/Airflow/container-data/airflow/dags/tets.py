from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    'prepare_dag',
    schedule_interval='@once',
    start_date=datetime(2019,11,21))

initT = BashOperator(
    task_id='init',
    bash_command='/usr/bin/init.sh',
    dag=dag
)

prepare_trainT = BashOperator(
    task_id='prepare_train',
    bash_command='/usr/bin/prepare_train.sh',
    dag=dag
)

prepare_testT= BashOperator(
    task_id='prepare_test',
    bash_command='/usr/bin/prepare_test.sh',
    dag=dag
)


initT >> [prepare_trainT, prepare_testT]
#initT >> prepare_testT
