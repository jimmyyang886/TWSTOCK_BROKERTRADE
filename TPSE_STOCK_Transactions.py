from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


import pendulum

local_tz = pendulum.timezone("Asia/Taipei")


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'email': ['jimmyyang886@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020, 10, 15 ,14, 0, tzinfo=local_tz),
}
 

dag = DAG('TPSE_Transactions_MySQL', description='Fetch TWSE to MySQL',
          default_args=default_args,
          #schedule_interval='0 14 * * *',
          schedule_interval='@daily',)



TPSE_Fetch_operator = BashOperator(task_id='TPSE_FETCH_task', 
        bash_command='/home/spark/TPSE/getTPSE.sh ', dag=dag)

#TWSE_Import_operator = BashOperator(task_id='TWSE_import_task', 
#        bash_command='/home/spark/PycharmProjects/Stock_Price_API/ImportTWSEprice_daily.sh ', dag=dag)

#TWSE_Fetch_operator >> TWSE_Import_operator  
TPSE_Fetch_operator 
