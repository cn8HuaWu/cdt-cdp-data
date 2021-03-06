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

entity = 'plan_rrp_rmb_portfolio'
src_entity = 'lgc_plan_rrp_rmb_portfolio'
DAG_NAME = 'lgc_plan_rrp_rmb_portfolio_dag'
src_file_sheet_name = ['Fixed_DP02','Floating_DP01','Floating_DP02','Floating_DP03','Floating_DP04','Floating_DP05','Floating_DP06','Floating_DP07','Floating_DP08','Floating_DP09','Floating_DP10','Floating_DP11','Floating_DP12']

sheet ={
"Fixed_DP02":{
    'start_column': 0,
    'column_width': 14
},
"Floating_DP01":{
    'start_column': 0,
    'column_width': 14
},
"Floating_DP02":{
    'start_column': 10,
    'column_width': 14
},
"Floating_DP03":{
    'start_column': 0,
    'column_width': 14
},
"Floating_DP04":{
    'start_column': 0,
    'column_width': 14
},
"Floating_DP05":{
    'start_column': 0,
    'column_width': 14
},
"Floating_DP06":{
    'start_column': 0,
    'column_width': 14
},
"Floating_DP07":{
    'start_column': 0,
    'column_width': 14
},
"Floating_DP08":{
    'start_column': 0,
    'column_width': 14
},
"Floating_DP09":{
    'start_column': 0,
    'column_width': 14
},
"Floating_DP010":{
    'start_column': 0,
    'column_width': 14
},
"Floating_DP011":{
    'start_column': 0,
    'column_width': 14
},
"Floating_DP012":{
    'start_column': 0,
    'column_width': 14
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

def add_new_column_with_value(row:list, input_file_path, sheetname, *args):
    if row is None:
        return row
    # append 1 new column based the sheetname
    row.append(sheetname)

    # insert 1 new column for the year version according the sheet name
    file_name = os.path.splitext(os.path.basename(input_file_path))[0]
    year = file_name.split("_")[-1]
    row.insert(2,year)
    return row

def load_src2stg(**kwargs):
    batch_date = kwargs.get('dag_run').conf.get('batch_date')
    src_filename = kwargs.get('dag_run').conf.get('src_filename')
    #
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    #
    OK_FILE_PATH  = kwargs.get('dag_run').conf.get('ok_file_path')
    excel_fun_list = [add_new_column_with_value]
    #src2stg = Src2stgHandler(STAGING, batch_date, SRC_NAME, entity, stg_suffix, src_filename, myutil, OK_FILE_PATH, excel_fun_list=excel_fun_list, has_head=False, sheetname=src_file_sheet_name, merge =False, **sheet)
    # ??????1???excel?????????????????????sheet??? ?????????**sheet ????????? ??????????????????header??? ???????????????????????????header??? ????????????**sheet??????
    src2stg = Src2stgHandler(STAGING, batch_date, SRC_NAME, entity, stg_suffix, src_filename, myutil, OK_FILE_PATH, excel_fun_list=excel_fun_list, has_head=False, read_all=True, merge = False)
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

preprocess_plan_rrp_rmb_portfolio_task = PythonOperator(
    task_id = 'preprocess_plan_rrp_rmb_portfolio_task',
    provide_context = True,
    python_callable = process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

plan_rrp_rmb_portfolio_src2stg_task = PythonOperator(
    task_id='plan_rrp_rmb_portfolio_src2stg_task',
    provide_context = True,
    python_callable = load_src2stg,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)


plan_rrp_rmb_portfolio_stg2ods_task = PythonOperator(
    task_id='plan_rrp_rmb_portfolio_stg2ods_task',
    provide_context = True,
    python_callable = load_stg2ods,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

# create edw data task:
edw_plan_rrp_rmb_portfolio_create = PythonOperator(
    task_id='edw_plan_rrp_rmb_portfolio_create',
    provide_context=True,
    python_callable=update_downstream,
    op_kwargs={'myutil': myutil, 'gpdb': db, 'sql_file_name': "lgc_plan_rrp_rmb_portfolio",
               'sql_section': 'create_edw_table_query', 'args': args},
    on_failure_callback=dag_failure_handler,
    dag=dag,
)

# delete edw data task:
# insert into edw data task:
edw_plan_rrp_rmb_portfolio_insert = PythonOperator(
    task_id='edw_plan_rrp_rmb_portfolio_insert',
    provide_context=True,
    python_callable=update_downstream,
    op_kwargs={'myutil': myutil, 'gpdb': db, 'sql_file_name': "lgc_plan_rrp_rmb_portfolio",
               'sql_section': 'insert_edw_table_query', 'args': args},
    on_failure_callback=dag_failure_handler,
    dag=dag,
)
postprocess_plan_rrp_rmb_portfolio_task = PythonOperator(
    task_id = 'postprocess_plan_rrp_rmb_portfolio_task',
    provide_context = True,
    python_callable = post_process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

preprocess_plan_rrp_rmb_portfolio_task >> plan_rrp_rmb_portfolio_src2stg_task >> plan_rrp_rmb_portfolio_stg2ods_task >> edw_plan_rrp_rmb_portfolio_create
edw_plan_rrp_rmb_portfolio_create >> edw_plan_rrp_rmb_portfolio_insert >> postprocess_plan_rrp_rmb_portfolio_task