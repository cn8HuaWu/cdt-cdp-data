from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.executors.sequential_executor import SequentialExecutor
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow import configuration as conf
from airflow import DAG

import logging
import os, sys
from pathlib import Path
from datetime import datetime

DAG_HOME =  Variable.get('dag_home').strip().rstrip('/')
sys.path.append(DAG_HOME + "/tasks/")
sys.path.append("../tasks/")
from utils.myutil import Myutil
from utils.db import Mydb
from subdags.subdag_sync_rds import sync_subdag
from update_downstream_table import update_downstream

# variable to run the shell scripts
SRC_NAME = "DL"

myutil = Myutil(DAG_HOME)
gp_host = myutil.get_conf( 'Greenplum', 'GP_HOST')
gp_port = myutil.get_conf( 'Greenplum', 'GP_PORT')
gp_db = myutil.get_conf( 'Greenplum', 'GP_DB')
gp_usr = myutil.get_conf( 'Greenplum', 'GP_USER')
gp_pw = myutil.get_conf( 'Greenplum', 'GP_PASSWORD')
db = Mydb(gp_host, gp_port, gp_db, gp_usr, gp_pw)
entity_conf = myutil.get_entity_config()
email_to_list =  Variable.get('email_to_list').split(',')
DAG_NAME = 'dl_scv_dag'
entity = 'scv'
src_entity = 'dl_scv'

batch_date = datetime.strftime(datetime.now(),'%Y%m%d')
dag_start_date = Variable.get('dag_start_date').strip()
scv_interval = Variable.get('interval_scv').strip()
def process_fileload(is_encrypted = False, is_compressed = False, **kwargs):
    kwargs['task_instance'].xcom_push(key='batch_date', value=batch_date)

def post_process_fileload( **kwargs):
    #rename: change prefix to "done-"
   pass

def dag_failure_handler(context):
    #rename: change prefix to "failed-"
    pass


args = {
    'owner': 'cdp_admin',
    'email': email_to_list,
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': datetime.strptime(dag_start_date,'%Y-%m-%d %H:%M:%S'),
    'max_active_runs': 1,
    "src_name": SRC_NAME,
    "batch_date_args": 'preprocess_scv_task'
}

dag = DAG(dag_id = DAG_NAME,
            default_args = args,
            concurrency = 1, 
            max_active_runs = 1, 
            schedule_interval = scv_interval)

preprocess_scv_task = PythonOperator(
    task_id = 'preprocess_scv_task',
    provide_context = True,
    python_callable = process_fileload,
    retries = 1,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

refresh_dl_scv_task = PythonOperator(
    task_id='refresh_dl_scv_task',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_scv" , 'sql_section': 'reload_dm_scv', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

scv_sync_2_rds_task = SubDagOperator(
    task_id='scv_sync_2_rds_task',
    subdag=sync_subdag(DAG_NAME, 'scv_sync_2_rds_task', myutil, entity_conf, args, entity),
    default_args=args,
    executor=SequentialExecutor(),
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

postprocess_scv_task = PythonOperator(
    task_id = 'postprocess_scv_task',
    provide_context = True,
    python_callable = post_process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

preprocess_scv_task >> refresh_dl_scv_task  >> scv_sync_2_rds_task >> postprocess_scv_task
