from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
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
from src2stg import Src2stgHandler
from stg2ods import Stg2odsHandler
from ods2edw import Ods2edwHandler
from utils.myutil import Myutil
from utils.db import Mydb
from subdags.subdag_sync_rds import sync_subdag
from update_downstream_table import update_downstream

# variable to run the shell scripts
SRC_NAME = "BU"
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
entity = 'mly_tm_traffic'
src_entity = 'bu_mly_tm_traffic'
DAG_NAME = 'bu_mly_tm_traffic_dag'

def process_fileload(is_encrypted = False, is_compressed = False, **kwargs):
    OK_FILE_PATH  = kwargs.get('dag_run').conf.get('ok_file_path')
    
    # remove the ok file and get the source file
    if( OK_FILE_PATH is None or not os.path.exists(OK_FILE_PATH) ):
        logging.error("OK_FILE_PATH: %s, ok file does not exist. ", OK_FILE_PATH)
        raise IOError("OK_FILE_PATH not found") 
    # else:
    #     os.remove(OK_FILE_PATH)

    if( not os.path.isfile(OK_FILE_PATH[:-3]) ):
        logging.error("Source file does not exist. File path: %s", OK_FILE_PATH[:-3])
        ## source file does not exist, set the Job failed
        raise IOError("Source file not found") 

    myutil.modify_ok_file_prefix( old_prefix=None, prefix="running", ok_file_path=OK_FILE_PATH)
    

def post_process_fileload( **kwargs):
    if ("skip_load"  in kwargs.get('dag_run').conf 
        and kwargs.get('dag_run').conf.get("skip_load").upper() == 'Y' ):
        return 
    #rename: change prefix to "done-"
    OK_FILE_PATH  = kwargs.get('dag_run').conf.get('ok_file_path')
    myutil.modify_ok_file_prefix("running", "done", OK_FILE_PATH)

def dag_failure_handler(context):
    #rename: change prefix to "failed-"
    OK_FILE_PATH  = context.get('dag_run').conf.get('ok_file_path')
    myutil.modify_ok_file_prefix("running", "failed",OK_FILE_PATH)


def load_src2stg(**kwargs):
    batch_date = kwargs.get('dag_run').conf.get('batch_date')
    src_filename = kwargs.get('dag_run').conf.get('src_filename')
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    OK_FILE_PATH  = kwargs.get('dag_run').conf.get('ok_file_path')
    src2stg = Src2stgHandler(STAGING, batch_date, SRC_NAME, entity, stg_suffix, src_filename, myutil, OK_FILE_PATH, has_head=False)
    src2stg.start()

def load_stg2ods(**kwargs):
    batch_date = kwargs.get('dag_run').conf.get('batch_date')
    pkey = entity_conf[src_entity]["key"]
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    stg2ods = Stg2odsHandler(TEMP_FOLDER, STAGING, ODS, batch_date, SRC_NAME, entity, stg_suffix, pkey, myutil, db, has_head=0 )
    stg2ods.start()


def load_ods2edw(**kwargs):
    batch_date = kwargs.get('dag_run').conf.get('batch_date')
    pkey = entity_conf[src_entity]["key"]
    table_prefix = entity_conf[src_entity]["edw_prefix"]
    update_type = entity_conf[src_entity]["update_type"]
    ods2edw = Ods2edwHandler(  batch_date, SRC_NAME, entity, pkey,table_prefix, myutil, db )
    ods2edw.start()

def b_choice(**kwargs):
    if ("skip_load"  in kwargs.get('dag_run').conf 
        and kwargs.get('dag_run').conf.get("skip_load").upper() == 'Y' ):
        return "mly_tm_traffic_dummy"
    else:
        return "preprocess_mly_tm_traffic_task"

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
            concurrency = 5, 
            max_active_runs = 1, 
            schedule_interval = None)

# branch_skip_load_mly_tm_traffic = BranchPythonOperator(
#     task_id='branch_skip_load_mly_tm_traffic',
#     python_callable= b_choice,
#     provide_context = True,
#     dag=dag,
# )

# mly_tm_traffic_dummy = DummyOperator(
#     task_id="mly_tm_traffic_dummy",
#     dag=dag,
# )

preprocess_mly_tm_traffic_task = PythonOperator(
    task_id = 'preprocess_mly_tm_traffic_task',
    provide_context = True,
    python_callable = process_fileload,
    retries = 1,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

mly_tm_traffic_src2stg_task = PythonOperator(
    task_id='mly_tm_traffic_src2stg_task',
    provide_context = True,
    python_callable = load_src2stg,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

mly_tm_traffic_stg2ods_task = PythonOperator(
    task_id='mly_tm_traffic_stg2ods_task',
    provide_context = True,
    python_callable = load_stg2ods,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

mly_tm_traffic_ods2edw_task = PythonOperator(
    task_id='mly_tm_traffic_ods2edw_task',
    provide_context = True,
    python_callable = load_ods2edw,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

# mly_tm_traffic_update_dl_mly_sales_rpt = PythonOperator(
#     task_id='mly_tm_traffic_update_dl_mly_sales_rpt',
#     provide_context = True,
#     python_callable = update_downstream,
#     op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_mly_sales_rpt" , 'sql_section': 'update_by_jd_and_tm_traffic', 'args': args},
#     on_failure_callback = dag_failure_handler,
#     trigger_rule='none_failed',
#     dag=dag,
# )


mly_tm_traffic_sync_2_rds_task = SubDagOperator(
    task_id='mly_tm_traffic_sync_2_rds_task',
    subdag=sync_subdag(DAG_NAME, 'mly_tm_traffic_sync_2_rds_task', myutil, entity_conf, args, entity),
    default_args=args,
    executor=SequentialExecutor(),
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

postprocess_mly_tm_traffic_task = PythonOperator(
    task_id = 'postprocess_mly_tm_traffic_task',
    provide_context = True,
    python_callable = post_process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

# branch_skip_load_mly_tm_traffic >> mly_tm_traffic_dummy >> mly_tm_traffic_update_dl_mly_sales_rpt
# branch_skip_load_mly_tm_traffic >> preprocess_mly_tm_traffic_task
preprocess_mly_tm_traffic_task >> mly_tm_traffic_src2stg_task >> mly_tm_traffic_stg2ods_task >> mly_tm_traffic_ods2edw_task 
mly_tm_traffic_ods2edw_task >> mly_tm_traffic_sync_2_rds_task >> postprocess_mly_tm_traffic_task
