from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
#from airflow.operators.sensors import ExternalTaskSensor
from TWSE_STOCK.FetchTWSEprice_Check_update import latestcheck



import pendulum


#import os
#import sys
#set required paths:

#os.environ['SPARK_HOME'] = '/home/spark/spark-2.4.5-bin-hadoop2.7'
#sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'bin'))

local_tz = pendulum.timezone("Asia/Taipei")


def branch_func(**kwargs):
    ti = kwargs['ti']
    xcom_value = ti.xcom_pull(key='update', task_ids='TWSE_update_check')
    print(xcom_value)
    if xcom_value == True:
        print('continue...')
        return 'continue_task'
    else:
        print('STOP!!')
        return 'stop_task'

def push(**kwargs):
    """Pushes an XCom without a specific target"""
    year=datetime.today().date().year
    month=datetime.today().date().month
    kwargs['ti'].xcom_push(key='update', value=latestcheck(year, month))



default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'email': ['jimmyyang886@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020, 10, 15 ,17, 10, tzinfo=local_tz),
    'provide_context':True,
}



dag = DAG('TWSE_Broker_Transactions_MySQL', description='Fetch TWSE Broker to MySQL',
          default_args=default_args,
          schedule_interval='10 17 * * *',
          catchup=False,)
          #schedule_interval='@daily',)


start_op = PythonOperator(
    task_id='TWSE_update_check',
    dag=dag,
    python_callable=push,
)

branch_op = BranchPythonOperator(
    task_id='branch_task',
    provide_context=True,
    python_callable=branch_func,
    dag=dag)

continue_op = DummyOperator(task_id='continue_task', dag=dag)
stop_op = DummyOperator(task_id='stop_task', dag=dag)

TWSE_Fetch_OP = BashOperator(task_id='TWSE_FETCH_Broker_task', 
        bash_command='/home/spark/PycharmProjects/TWSTOCK_BROKERTRADE/FetchTWSE_Broker.sh ', dag=dag)

TWSE_Import_OP = BashOperator(task_id='TWSE_Broker_import_task', 
        bash_command='/home/spark/PycharmProjects/TWSTOCK_BROKERTRADE/csv2MySQL.sh ', dag=dag)

TWSE_ImportHDFS_OP = BashOperator(task_id='TWSE_Broker_ImportHDFS_task', 
        bash_command='/home/spark/PythonProjects/twstock_ETL_spark/import_broker_hdfs_daily.sh ', dag=dag)


#_config ={'application':"${SPARK_HOME}//home/spark/PythonProjects/twstock_ETL_spark/import_broker_hdfs_daily.py",
        #'py_files' : '/home/spark/PythonProjects/twstock_ETL_spark/import_broker_hdfs_daily.py',
#        'master' : 'spark://master:7077',
        #'deploy-mode' : 'cluster',
#        'executor_cores': 1,
#        'EXECUTORS_MEM': '1G'
#        }

#TWSE_ImportHDFS_OP = SparkSubmitOperator(
#    task_id='TWSE_Broker_ImportHDFS_task',
#    dag=dag,
#    **_config)

#proxymysql_OP = BashOperator(task_id='proxy_update_task', 
#        bash_command='/home/spark/PycharmProjects/Proxy2mySQL/proxy_get.sh ', dag=dag)

#proxymysql_start_OP = BashOperator(task_id='proxy_update_addcron_task', 
#        bash_command='/home/spark/PycharmProjects/Proxy2mySQL/proxy_get_addcron.sh ', dag=dag)

#proxymysql_stop_OP = BashOperator(task_id='proxy_update_rmcron_task', 
#        bash_command='/home/spark/PycharmProjects/Proxy2mySQL/proxy_get_rmcron.sh ', dag=dag,
#        trigger_rule='none_skipped')

start_op >> branch_op >> [continue_op, stop_op]
continue_op >> TWSE_Fetch_OP >> TWSE_Import_OP >> TWSE_ImportHDFS_OP 

