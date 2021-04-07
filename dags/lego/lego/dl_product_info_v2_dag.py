from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.executors.sequential_executor import SequentialExecutor
from airflow.models import Variable
from airflow import DAG
from airflow.utils.db import provide_session
from airflow.models import XCom

import logging
import re
import os, sys
from datetime import datetime

DAG_HOME =  Variable.get('dag_home').strip().rstrip('/')
sys.path.append("../tasks/")
sys.path.append(DAG_HOME + "/tasks/")

from src2stg import Src2stgHandler
from stg2ods import Stg2odsHandler
from ods2edw import Ods2edwHandler
from utils.myutil import Myutil
from subdags.subdag_sync_rds import sync_subdag
from update_downstream_table import update_downstream

# variable to run the shell scripts
SRC_NAME = "DL"
STAGING = 'Staging'
ODS = 'ODS'
TEMP_FOLDER='Temp'

entity = 'product_info_v2'
src_entity = 'dl_product_info_v2'
DAG_NAME = 'dl_product_info_v2_dag'
email_to_list =  Variable.get('email_to_list').split(',')

myutil = Myutil(dag_home=DAG_HOME, entity_name=src_entity)
db = myutil.get_db()
entity_conf = myutil.get_entity_config()

dag_start_date = Variable.get('dag_start_date').strip()
# product_batchdate = datetime.strftime(datetime.now(),'%Y%m%d')
update_attributions = ['update_product_sales_category', 'update_product_core_line']

args = {
    'owner': 'cdp_admin',
    'email': email_to_list,
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'max_active_runs': 1,
    'retries': 0,
    "src_name": SRC_NAME,
    "start_date": datetime.strptime(dag_start_date,'%Y-%m-%d %H:%M:%S'),
   
}

dag = DAG(dag_id = DAG_NAME,
            default_args = args,
            concurrency = 1, 
            max_active_runs = 1,
            catchup=False, 
            schedule_interval = None,
)

def process_fileload(is_encrypted = False, is_compressed = False, **kwargs):
    # logging.info("current path: " + os.getcwd())
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

def run_composit_task(query_sections:list, **kwargs):
    sql_dict = myutil.get_sql_yml_fd(src_entity)
    # batch_date = kwargs.get('dag_run').conf.get('batch_date')
    with db.create_session() as session:
        for set in query_sections:
            query = (sql_dict['EDW'][set])
            session.execute(query)

@provide_session
def cleanup_xcom(context, session=None):
    session.query(XCom).filter(XCom.dag_id == DAG_NAME).delete()

def post_process_fileload( **kwargs):
    cleanup_xcom(kwargs)
    if ("skip_load"  in kwargs.get('dag_run').conf 
        and kwargs.get('dag_run').conf.get("skip_load").upper() == 'Y' ):
        return 

    #rename: change prefix to "done-"
    OK_FILE_PATH  = kwargs.get('dag_run').conf.get('ok_file_path')
    myutil.modify_ok_file_prefix("running", "done", OK_FILE_PATH)

def dag_failure_handler(context):
    cleanup_xcom(context)
    OK_FILE_PATH = context.get('dag_run').conf.get('ok_file_path')
    myutil.modify_ok_file_prefix("running", "failed", OK_FILE_PATH)
    
# 在文件的第一列插入product的年份
def add_yearversion( row:list , excel_path, *args ):
    if row is None:
        return row
    filename = os.path.basename(excel_path)
    rs = re.match('(\\d{4}).*', filename)
    if rs:
        row.insert(0, rs.group(1))
    return row

# china launch date 可能是非日期格式的
def  format_cn_launch_date(row:list, *args):
    if row is None:
        return row
    lcs_date = row[15]
    cn_launch_date = row[14]
    if re.match('\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}', lcs_date):
        row[14] = lcs_date
        return row
    if re.match('\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}', cn_launch_date):
        return row
    else:
        rs = re.match('.*(\\d{2}/\\d{2}/\\d{4}).*', cn_launch_date, re.M)
        if rs:
            cn_data = rs.group(1)
            cn_data_list = list(reversed(cn_data.split('/')))
            row[14] = '-'.join(cn_data_list)
            
    return row 

def load_stg2ods(**kwargs):
    pkey = entity_conf[src_entity]["key"]
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    batch_date = kwargs.get('dag_run').conf.get('batch_date')
    # my_batch_date = kwargs['task_instance'].xcom_pull(key='batch_date', task_ids='branch_external_trigger')
    stg2ods = Stg2odsHandler(TEMP_FOLDER, STAGING, ODS, batch_date, SRC_NAME, entity, stg_suffix, pkey, myutil, db, has_head=0)
    stg2ods.start()     

def load_src2stg(**kwargs):
    batch_date = kwargs.get('dag_run').conf.get('batch_date')
    src_filename = kwargs.get('dag_run').conf.get('src_filename')
    #
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    #
    OK_FILE_PATH  = kwargs.get('dag_run').conf.get('ok_file_path')
    excel_fun_list = [add_yearversion, format_cn_launch_date, myutil.rearrange_columns ]
    src2stg = Src2stgHandler(STAGING, batch_date, SRC_NAME, entity, stg_suffix, src_filename, myutil, OK_FILE_PATH, excel_fun_list=excel_fun_list, has_head=True, excel_skip_row=2, sheetname='China BU Product Plan', merge =False)
    src2stg.start(version='v2')

def load_ods2edw(**kwargs):
    pkey = entity_conf[src_entity]["key"]
    table_prefix = entity_conf[src_entity]["edw_prefix"]
    batch_date = kwargs.get('dag_run').conf.get('batch_date')
    ods2edw = Ods2edwHandler( batch_date, SRC_NAME, entity, pkey,table_prefix, myutil, db )
    ods2edw.start()

preprocess_product_info_v2_task = PythonOperator(
    task_id = 'preprocess_product_info_v2_task',
    provide_context = True,
    python_callable = process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

product_info_v2_src2stg_task = PythonOperator(
    task_id='product_info_v2_src2stg_task',
    provide_context = True,
    python_callable = load_src2stg,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

product_info_stg2ods_v2_task = PythonOperator(
    task_id='product_info_stg2ods_v2_task',
    provide_context = True,
    python_callable = load_stg2ods,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

product_info_ods2edw_v2_task = PythonOperator(
    task_id='product_info_ods2edw_v2_task',
    provide_context = True,
    python_callable = load_ods2edw,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

product_info_v2_update_task = PythonOperator(
    task_id='product_info_v2_update_task',
    provide_context = True,
    python_callable = run_composit_task,
    op_kwargs = {'query_sections': update_attributions},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

product_info_v2_sync_2_rds_task = SubDagOperator(
    task_id='product_info_v2_sync_2_rds_task',
    subdag=sync_subdag(DAG_NAME, 'product_info_v2_sync_2_rds_task', myutil, entity_conf, args, entity),
    default_args=args,
    executor=SequentialExecutor(),
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

postprocess_product_info_v2_task = PythonOperator(
    task_id = 'postprocess_product_info_v2_task',
    provide_context = True,
    python_callable = post_process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)
preprocess_product_info_v2_task >> product_info_v2_src2stg_task >> product_info_stg2ods_v2_task >> product_info_ods2edw_v2_task 
product_info_ods2edw_v2_task >> product_info_v2_update_task >> product_info_v2_sync_2_rds_task  >> postprocess_product_info_v2_task