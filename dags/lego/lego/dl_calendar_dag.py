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
import imp

DAG_HOME =  Variable.get('dag_home').strip().rstrip('/')
sys.path.append(DAG_HOME + "/tasks/")
sys.path.append("../tasks/")

from src2stg import Src2stgHandler
from stg2ods import Stg2odsHandler
from ods2edw import Ods2edwHandler
from utils.myutil import Myutil
from subdags.subdag_sync_rds import sync_subdag

# variable to run the shell scripts
SRC_NAME = "DL"
STAGING = 'Staging'
ODS = 'ODS'
TEMP_FOLDER='Temp'

DAG_NAME = 'dl_calendar_dag'
entity = 'calendar'
src_entity = 'dl_calendar'

myutil = Myutil(dag_home=DAG_HOME, entity_name=src_entity)
db = myutil.get_db()
entity_conf = myutil.get_entity_config()
email_to_list =  Variable.get('email_to_list').split(',')
calendar_year =  Variable.get('calendar_year').strip()
monitor_path = Variable.get('monitor_path')
if calendar_year == '':
    calendar_year = datetime.now().strftime('%Y')

target_path = os.path.join(monitor_path, "input/LEGO/calendar",calendar_year +"calendar.csv")
batch_date = calendar_year

def process_fileload(is_encrypted = False, is_compressed = False, **kwargs):
    calenda_gen = imp.load_source("calenda_gen", os.path.join(DAG_HOME, "/tasks/utils/calendar_gen.py"))
    calenda_gen.start(calendar_year, target_path)

def post_process_fileload( **kwargs):
   pass
def dag_failure_handler(context):
    #rename: change prefix to "failed-"
   pass

def load_src2stg(**kwargs):
    # batch_date = kwargs.get('dag_run').conf.get('batch_date')
    src_filename = target_path
    #
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    #
    OK_FILE_PATH  = kwargs.get('dag_run').conf.get('ok_file_path')
    src2stg = Src2stgHandler(STAGING, batch_date, SRC_NAME, entity, stg_suffix, src_filename, myutil, OK_FILE_PATH, has_head=False, sheetname='Sheet1', merge =False)
    src2stg.start(version='v2')

def load_stg2ods(**kwargs):
    pkey = entity_conf[src_entity]["key"]
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    #
    batch_date = kwargs.get('dag_run').conf.get('batch_date')
    stg2ods = Stg2odsHandler(TEMP_FOLDER, STAGING, ODS, batch_date, SRC_NAME, entity, stg_suffix, pkey, myutil, db, has_head = False )
    stg2ods.start()



def load_ods2edw(**kwargs):
    # batch_date = kwargs.get('dag_run').conf.get('batch_date')
    pkey = entity_conf[src_entity]["key"]
    table_prefix = entity_conf[src_entity]["edw_prefix"]
    update_type = entity_conf[src_entity]["update_type"]
    ods2edw = Ods2edwHandler(  batch_date, SRC_NAME, entity, pkey,table_prefix, myutil, db )
    ods2edw.start()

args = {
    'owner': 'cdp_admin',
    'email': email_to_list,
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': days_ago(0),
    'max_active_runs': 1,
    'retries': 0,
    "src_name": SRC_NAME
}

dag = DAG(dag_id = DAG_NAME,
            default_args = args,
            concurrency = 1, 
            max_active_runs = 1, 
            schedule_interval = None)

preprocess_calendar_task = PythonOperator(
    task_id = 'preprocess_calendar_task',
    provide_context = True,
    python_callable = process_fileload,
    retries = 1,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

calendar_src2stg_task = PythonOperator(
    task_id='calendar_src2stg_task',
    provide_context = True,
    python_callable = load_src2stg,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

calendar_stg2ods_task = PythonOperator(
    task_id='calendar_stg2ods_task',
    provide_context = True,
    python_callable = load_stg2ods,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

calendar_ods2edw_task = PythonOperator(
    task_id='calendar_ods2edw_task',
    provide_context = True,
    python_callable = load_ods2edw,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

calendar_sync_2_rds_task = SubDagOperator(
    task_id='calendar_sync_2_rds_task',
    subdag=sync_subdag(DAG_NAME, 'calendar_sync_2_rds_task', myutil, entity_conf, args, entity),
    default_args=args,
    executor=SequentialExecutor(),
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

postprocess_calendar_task = PythonOperator(
    task_id = 'postprocess_calendar_task',
    provide_context = True,
    python_callable = post_process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

preprocess_calendar_task >> calendar_src2stg_task >> calendar_stg2ods_task >> calendar_ods2edw_task 
calendar_ods2edw_task

# >> calendar_sync_2_rds_task >> postprocess_calendar_task
