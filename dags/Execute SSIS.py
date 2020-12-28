from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'catchup':False,
    # 'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
dag = DAG(
    'SSIS_execution',
    default_args=default_args,
    description='A simple test DAG to execute SSIS',
    schedule_interval='1 * * * *',
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    #depends_on_past=True,
    dag=dag,
)


t2 = MsSqlOperator(
    task_id='ssis_delayed',
    mssql_conn_id='mssql_default',
    sql='USE [SSISDB];EXECUTE AS LOGIN = \'EDGOCH-27062019\edgoch\';EXEC DataMart.executeSSIS  \'Delayed.dtsx\', \'Integration\', \'Development\'',
    autocommit=True,
    retries=0,
    #depends_on_past=True,
    dag=dag,
    ) 

t3 = MsSqlOperator(
    task_id='ssis_delayed_fail',
    mssql_conn_id='mssql_default',
    sql='USE [SSISDB];EXECUTE AS LOGIN = \'EDGOCH-27062019\edgoch\';EXEC DataMart.executeSSIS  \'Delayed Fail.dtsx\', \'Integration\', \'Development\'',
    autocommit=True,
    retries=1,
    #depends_on_past=True,
    dag=dag,
    ) 

t4 = MsSqlOperator(
    task_id='ssis_drop_delay_load',
    mssql_conn_id='mssql_default',
    sql='USE [SSISDB];EXECUTE AS LOGIN = \'EDGOCH-27062019\edgoch\';EXEC DataMart.executeSSIS  \'Drop Delay Load.dtsx\', \'Integration\', \'Development\'',
    autocommit=True,
    retries=0,
    #depends_on_past=True,
    dag=dag,
    ) 

t1 >> [t2, t4, t3]