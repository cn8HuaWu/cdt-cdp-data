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

from stg2ods import Stg2odsHandler
from ods2edw import Ods2edwHandler
from utils.myutil import Myutil
from utils.db import Mydb
from subdags.subdag_sync_rds import sync_subdag
from subdags.subdag_post_update_edw import update_subdag

# variable to run the shell scripts
SRC_NAME = "TM"
STAGING = 'Staging'
ODS = 'ODS'
TEMP_FOLDER='Temp'


myutil = Myutil(DAG_HOME)
gp_host = myutil.get_conf( 'Greenplum', 'GP_HOST')
gp_port = myutil.get_conf( 'Greenplum', 'GP_PORT')
gp_db = myutil.get_conf( 'Greenplum', 'GP_DB')
gp_usr = myutil.get_conf( 'Greenplum', 'GP_USER')
gp_pw = myutil.get_conf( 'Greenplum', 'GP_PASSWORD')
db = Mydb(gp_host, gp_port, gp_db, gp_usr, gp_pw)
entity_conf = myutil.get_entity_config()
email_to_list =  Variable.get('email_to_list').split(',')

entity = 'order_dtl_2018_his'
src_entity = 'tm_order_dtl_2018_his'
batch_date = datetime.strftime(datetime.now(), '%Y%m%d')
DAG_NAME = 'tm_order_dtl_2018_his_dag'
history_path = Variable.get('tm_historical_path')
data_file_list = Variable.get('tm_2018_historical_path').split(",")
sql_file_name = SRC_NAME.lower()+"_"+entity

def process_fileload(is_encrypted = False, is_compressed = False, **kwargs):
    
    bucket = myutil.get_oss_bucket()
    kwargs['task_instance'].xcom_push(key='batch_date', value=batch_date)

    stg_path = '/'.join((STAGING, SRC_NAME, entity, batch_date))
    timestamp_str =  datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    for fd in data_file_list:
        # convert excel to csv
        input_file_path = os.path.join(history_path, fd)

        # backup the history file
        backup_path ='/'.join( ('Backup', SRC_NAME, entity, timestamp_str + '_' + fd) )
        myutil.upload_local_oss(bucket, input_file_path, backup_path)

        filepath,fullflname =  os.path.split(fd)
        fname,ext = os.path.splitext(fullflname)
        output_path = input_file_path
        if ( ext.lower() in ('.xlsx', '.xls')):
            fname = fname + ".csv"
            output_path = os.path.join(filepath, fname)
            myutil.read_excel_file(input_file_path, output_path, keephead=False) 
        elif (ext.lower() == '.csv'):
            fname = os.path.basename(fd)
        else:
            raise ValueError("The product info must be excel or csv")

        myutil.upload_local_oss(bucket, output_path, stg_path + "/" + fname)
        os.remove(output_path)

def post_process_fileload( **kwargs):
    #rename: change prefix to "done-"
    pass

def dag_failure_handler(context):
    #rename: change prefix to "failed-"
    pass

def load_stg2ods(**kwargs):
    pkey = entity_conf[src_entity]["key"]
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    my_batch_date = kwargs['task_instance'].xcom_pull(key='batch_date', task_ids='preprocess_tm_order_dtl_2018_his_task')
    stg2ods = Stg2odsHandler(TEMP_FOLDER, STAGING, ODS, my_batch_date, SRC_NAME, entity, stg_suffix, pkey, myutil, db, has_head=0 )
    stg2ods.start()


def load_ods2edw(**kwargs):
    pkey = entity_conf[src_entity]["key"]
    table_prefix = entity_conf[src_entity]["edw_prefix"]
    update_type = entity_conf[src_entity]["update_type"]
    my_batch_date = kwargs['task_instance'].xcom_pull(key='batch_date', task_ids='preprocess_tm_order_dtl_2018_his_task')
    ods2edw = Ods2edwHandler(  my_batch_date, SRC_NAME, entity, pkey,table_prefix, myutil, db )
    ods2edw.start()

args = {
    'owner': 'cdp_admin',
    'email': email_to_list,
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': days_ago(0),
    'max_active_runs': 1,
    "src_name": SRC_NAME
}

dag = DAG(dag_id = DAG_NAME,
            default_args = args,
            concurrency = 1, 
            max_active_runs = 1, 
            schedule_interval = None)

preprocess_tm_order_dtl_2018_his_task = PythonOperator(
    task_id = 'preprocess_tm_order_dtl_2018_his_task',
    provide_context = True,
    python_callable = process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

tm_order_dtl_2018_his_stg2ods_task = PythonOperator(
    task_id='tm_order_dtl_2018_his_stg2ods_task',
    provide_context = True,
    python_callable = load_stg2ods,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

tm_order_dtl_2018_his_ods2edw_task = PythonOperator(
    task_id='tm_order_dtl_2018_his_ods2edw_task',
    provide_context = True,
    python_callable = load_ods2edw,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

postprocess_tm_order_dtl_2018_his_task = PythonOperator(
    task_id = 'postprocess_tm_order_dtl_2018_his_task',
    provide_context = True,
    python_callable = post_process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

preprocess_tm_order_dtl_2018_his_task >> tm_order_dtl_2018_his_stg2ods_task >> tm_order_dtl_2018_his_ods2edw_task >>  postprocess_tm_order_dtl_2018_his_task