from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow import DAG

import logging
import os, sys

DAG_HOME =  Variable.get('dag_home').strip().rstrip('/')
sys.path.append("../tasks/")
sys.path.append(DAG_HOME + "/tasks/")

from src2stg_test import src2stgHandler
from stg2ods import Stg2odsHandler
from ods2edw import Ods2edwHandler
from utils.myutil_test import Myutil
from utils.db import Mydb

# variable to run the shell scripts
SRC_NAME = "lgc"
STAGING = 'Staging'
ODS = 'ODS'
TEMP_FOLDER='Temp'

myutil_test = myutil_test(DAG_HOME)
gp_host = myutil_test.get_conf( 'Greenplum', 'GP_HOST')
gp_port = myutil_test.get_conf( 'Greenplum', 'GP_PORT')
gp_db = myutil_test.get_conf( 'Greenplum', 'GP_DB')
gp_usr = myutil_test.get_conf( 'Greenplum', 'GP_USER')
gp_pw = myutil_test.get_conf( 'Greenplum', 'GP_PASSWORD')
db = Mydb(gp_host, gp_port, gp_db, gp_usr, gp_pw)
entity_conf = myutil_test.get_entity_config()
email_to_list =  Variable.get('email_to_list').split(',')
entity = 'sales_stock_rrp_rmb'
src_entity = 'lgc_sales_stock_rrp_rmb'
DAG_NAME = 'lgc_sales_stock_rrp_rmb_dag_test'


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

    myutil_test.modify_ok_file_prefix( old_prefix=None, prefix="running", ok_file_path=OK_FILE_PATH)
    
def post_process_fileload( **kwargs):
    #rename: change prefix to "done-"
    if ("skip_load"  in kwargs.get('dag_run').conf 
        and kwargs.get('dag_run').conf.get("skip_load").upper() == 'Y' ):
        return 
    OK_FILE_PATH  = kwargs.get('dag_run').conf.get('ok_file_path')
    myutil_test.modify_ok_file_prefix("running", "done", OK_FILE_PATH)

def dag_failure_handler(context):
    #rename: change prefix to "failed-"
    OK_FILE_PATH  = context.get('dag_run').conf.get('ok_file_path')
    myutil_test.modify_ok_file_prefix("running", "failed",OK_FILE_PATH)
    
def load_src2stg_test(**kwargs):
    batch_date = kwargs.get('dag_run').conf.get('batch_date')
    src_filename = kwargs.get('dag_run').conf.get('src_filename')
    #
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    #
    OK_FILE_PATH  = kwargs.get('dag_run').conf.get('ok_file_path')
    src2stg_test = src2stg_testHandler(STAGING, batch_date, SRC_NAME, entity, stg_suffix, src_filename, myutil_test, OK_FILE_PATH, sheetname='Sheet1')
    src2stg_test.start()

def load_stg2ods(**kwargs):
    
    pkey = entity_conf[src_entity]["key"]
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    #
    kwargs['task_instance'].xcom_push(key='batch_date', value=batch_date)
    stg2ods = Stg2odsHandler(TEMP_FOLDER, STAGING, ODS, batch_date, SRC_NAME, entity, stg_suffix, pkey, myutil_test, db, has_head = has_head )
    stg2ods.start()


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

preprocess_sales_stock_rrp_rmb_task = PythonOperator(
    task_id = 'preprocess_sales_stock_rrp_rmb_task',
    provide_context = True,
    python_callable = process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

sales_stock_rrp_rmb_src2stg_test_task = PythonOperator(
    task_id='sales_stock_rrp_rmb_src2stg_test_task',
    provide_context = True,
    python_callable = load_src2stg_test,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)


phy_store_stg2ods_task = PythonOperator(
    task_id='phy_store_stg2ods_task',
    provide_context = True,
    python_callable = load_stg2ods,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

preprocess_sales_stock_rrp_rmb_task >> sales_stock_rrp_rmb_src2stg_test_task >> phy_store_stg2ods_task
