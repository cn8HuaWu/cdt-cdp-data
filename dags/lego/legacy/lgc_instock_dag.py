from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow import DAG

import logging
import os, sys

DAG_HOME =  Variable.get('dag_home').strip().rstrip('/')
sys.path.append("../tasks/")
sys.path.append(DAG_HOME + "/tasks/")

from src2stg import Src2stgHandler
from stg2ods import Stg2odsHandler
from ods2edw import Ods2edwHandler
from utils.myutil import Myutil
from utils.db import Mydb
from update_downstream_table import update_downstream

# variable to run the shell scripts
SRC_NAME = "lgc"
STAGING = 'Staging'
ODS = 'ODS'
TEMP_FOLDER='Temp'

entity = 'instock'
src_entity = 'lgc_instock'
DAG_NAME = 'lgc_instock_dag'

sheet ={
"Sheet1":{
    'start_column':0,
    'column_width':12
}
}

myutil = Myutil(dag_home=DAG_HOME, entity_name=src_entity)
db = myutil.get_db()
entity_conf = myutil.get_entity_config()
email_to_list =  Variable.get('email_to_list').split(',')

def process_fileload(is_encrypted = False, is_compressed = False, **kwargs):
    logging.info("current path: " + os.getcwd())
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
    #rename: change prefix to "done-"
    if ("skip_load"  in kwargs.get('dag_run').conf 
        and kwargs.get('dag_run').conf.get("skip_load").upper() == 'Y' ):
        return 
    OK_FILE_PATH  = kwargs.get('dag_run').conf.get('ok_file_path')
    myutil.modify_ok_file_prefix("running", "done", OK_FILE_PATH)

def dag_failure_handler(context):
    #rename: change prefix to "failed-"
    OK_FILE_PATH  = context.get('dag_run').conf.get('ok_file_path')
    myutil.modify_ok_file_prefix("running", "failed",OK_FILE_PATH)
    
def load_src2stg(**kwargs):
    batch_date = kwargs.get('dag_run').conf.get('batch_date')
    src_filename = kwargs.get('dag_run').conf.get('src_filename')
    #
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    #
    OK_FILE_PATH  = kwargs.get('dag_run').conf.get('ok_file_path')
    excel_fun_list = [myutil.rearrange_columns]
    src2stg = Src2stgHandler(STAGING, batch_date, SRC_NAME, entity, stg_suffix, src_filename, myutil, OK_FILE_PATH, excel_fun_list=excel_fun_list, has_head=False, sheetname='Sheet1', merge =False,excel_skip_row=1,**sheet)
    src2stg.start(version='v2')

def load_stg2ods(**kwargs):
    
    pkey = entity_conf[src_entity]["key"]
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    #
    batch_date = kwargs.get('dag_run').conf.get('batch_date')
    stg2ods = Stg2odsHandler(TEMP_FOLDER, STAGING, ODS, batch_date, SRC_NAME, entity, stg_suffix, pkey, myutil, db, has_head = False )
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

preprocess_instock_task = PythonOperator(
    task_id = 'preprocess_instock_task',
    provide_context = True,
    python_callable = process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

instock_src2stg_task = PythonOperator(
    task_id='instock_src2stg_task',
    provide_context = True,
    python_callable = load_src2stg,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)


instock_stg2ods_task = PythonOperator(
    task_id='instock_stg2ods_task',
    provide_context = True,
    python_callable = load_stg2ods,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

# create edw data task:
edw_lgc_instock_create = PythonOperator(
    task_id='edw_lgc_instock_create',
    provide_context=True,
    python_callable=update_downstream,
    op_kwargs={'myutil': myutil, 'gpdb': db, 'sql_file_name': "lgc_instock",
               'sql_section': 'create_edw_table_query', 'args': args},
    on_failure_callback=dag_failure_handler,
    dag=dag,
)

# delete edw data task:
# insert into edw data task:
edw_lgc_instock_insert = PythonOperator(
    task_id='edw_lgc_instock_insert',
    provide_context=True,
    python_callable=update_downstream,
    op_kwargs={'myutil': myutil, 'gpdb': db, 'sql_file_name': "lgc_instock",
               'sql_section': 'insert_edw_table_query', 'args': args},
    on_failure_callback=dag_failure_handler,
    dag=dag,
)
postprocess_instock_task = PythonOperator(
    task_id = 'postprocess_instock_task',
    provide_context = True,
    python_callable = post_process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

preprocess_instock_task >> instock_src2stg_task >> instock_stg2ods_task >> edw_lgc_instock_create
edw_lgc_instock_create >> edw_lgc_instock_insert >> postprocess_instock_task
