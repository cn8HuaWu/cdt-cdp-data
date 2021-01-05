from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow import DAG

import logging
import os, sys
from datetime import datetime

DAG_HOME =  Variable.get('dag_home').strip().rstrip('/')
sys.path.append("../tasks/")
sys.path.append(DAG_HOME + "/tasks/")

from utils.myutil import Myutil
from utils.jd_data_ingestion import start_ingest
from datetime import timedelta

DAG_NAME = 'dl_api_extract_jd_pop_consigneel_dag'
src_entity = 'jd_pop_consignee'
api_interval = Variable.get('interval_jd_api').strip()
dag_start_date = Variable.get('dag_start_date').strip()
data_timedelta = int(Variable.get('jd_data_timedelta').strip())
jd_load_timedelta = int(Variable.get('jd_load_timedelta').strip())

myutil = Myutil(DAG_HOME)
email_to_list =  Variable.get('email_to_list').split(',')

args = {
    'owner': 'cdp_admin',
    'email': email_to_list,
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date' : datetime.strptime(dag_start_date,'%Y-%m-%d %H:%M:%S'),
    'max_active_runs': 1,
}

dag = DAG(dag_id = DAG_NAME,
            default_args = args,
            concurrency = 1, 
            max_active_runs = 1, 
            
            schedule_interval = api_interval,
        )

def dag_failure_handler(context):
   pass


def start_ingest_data(**kwargs):
    batch_date = None
    days_to_load = jd_load_timedelta
    if ( kwargs.get('dag_run').conf is not None):
        if ('batch_date' in kwargs.get('dag_run').conf ):
            batch_date = kwargs.get('dag_run').conf.get('batch_date')
       
        if ( 'days_to_load' in kwargs.get('dag_run').conf ):
            days_to_load = int(kwargs.get('dag_run').conf.get('days_to_load'))
            
    start_ingest(src_entity, myutil, days_to_load = days_to_load, data_day_delta = data_timedelta, batch_date=batch_date)

extract_jd_pop_consignee_task = PythonOperator(
    task_id='extract_jd_pop_consignee_task',
    provide_context = True,
    python_callable = start_ingest_data,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

