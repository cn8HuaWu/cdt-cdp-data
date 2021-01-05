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
from src2stg import Src2stgHandler
from stg2ods import Stg2odsHandler
from ods2edw import Ods2edwHandler
from utils.myutil import Myutil
from utils.db import Mydb
from subdags.subdag_sync_rds import sync_subdag
from subdags.subdag_post_update_edw import update_subdag
from update_downstream_table import update_downstream

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

entity = 'member_1st_purchase_his'
src_entity = 'tm_member_1st_purchase_his'
DAG_NAME = 'tm_member_1st_purchase_his_dag'

def process_fileload(is_encrypted = False, is_compressed = False, **kwargs):

    # OK file trigger
    OK_FILE_PATH  = kwargs.get('dag_run').conf.get('ok_file_path')
    
    # remove the ok file and get the source file
    if( OK_FILE_PATH is None or not os.path.exists(OK_FILE_PATH) ):
        logging.error("OK_FILE_PATH: %s, ok file does not exist. ", OK_FILE_PATH)
        raise IOError("OK_FILE_PATH not found") 

    if( not os.path.isfile(OK_FILE_PATH[:-3]) ):
        logging.error("Source file does not exist. File path: %s", OK_FILE_PATH[:-3])
        ## source file does not exist, set the Job failed
        raise IOError("Source file not found") 

# convert the source file encoding and encrypt the mobile
    ok_dir_path = os.path.dirname(OK_FILE_PATH)
    src_filename = kwargs.get('dag_run').conf.get('src_filename')
    src_file_path = os.path.join(ok_dir_path, src_filename)

    src_encoding = myutil.detect_file_encoding(src_file_path)
    myutil.convert_file_encode(src_file_path, src_encoding.lower())
# end file encoding conversion

# encrypt the mobile field
    entity_conf = myutil.get_entity_config()
    to_encrypt_list = entity_conf[src_entity]['to_encrypt_columns'].split(",")
    dl_aes_key = myutil.get_dl_aes_key()
    dl_aes_iv = myutil.get_dl_aes_iv()

    sql_dict = myutil.get_sql_yml_fd(src_entity)
    columns_list = sql_dict['Staging']['src_columns'].replace(' text','').replace(' ','').split(",")
    myutil.encrypt_csv_fields(src_file_path, src_file_path,0, to_encrypt_list, columns_list, dl_aes_key, dl_aes_iv, del_src= True, keepheader=False)

# end encrypt the mobile field

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

    src2stg = Src2stgHandler(
            STAGING, 
            batch_date, 
            SRC_NAME, 
            entity, 
            stg_suffix, 
            src_filename, 
            myutil, 
            OK_FILE_PATH, 
            has_batchdate = True,
            src_entity = src_entity,
            entity_conf = entity_conf,
            is_encrypted = False,
            has_head=True,
            need_encrypt=False
            )
    src2stg.start()

def load_stg2ods(**kwargs):
    pkey = entity_conf[src_entity]["key"]
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    batch_date = kwargs.get('dag_run').conf.get('batch_date')
    stg2ods = Stg2odsHandler(TEMP_FOLDER, STAGING, ODS, batch_date, SRC_NAME, entity, stg_suffix, pkey, myutil, db, has_head=0 )
    stg2ods.start()


def load_ods2edw(**kwargs):
    pkey = entity_conf[src_entity]["key"]
    table_prefix = entity_conf[src_entity]["edw_prefix"]
    update_type = entity_conf[src_entity]["update_type"]
    batch_date = kwargs.get('dag_run').conf.get('batch_date')
    ods2edw = Ods2edwHandler( batch_date, SRC_NAME, entity, pkey,table_prefix, myutil, db, update_type=update_type)
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

preprocess_tm_member_1st_purchase_his_task = PythonOperator(
    task_id = 'preprocess_tm_member_1st_purchase_his_task',
    provide_context = True,
    python_callable = process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

tm_member_1st_purchase_his_src2stg_task = PythonOperator(
    task_id='tm_member_1st_purchase_his_src2stg_task',
    provide_context = True,
    python_callable = load_src2stg,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

tm_member_1st_purchase_his_stg2ods_task = PythonOperator(
    task_id='tm_member_1st_purchase_his_stg2ods_task',
    provide_context = True,
    python_callable = load_stg2ods,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

tm_member_1st_purchase_his_ods2edw_task = PythonOperator(
    task_id='tm_member_1st_purchase_his_ods2edw_task',
    provide_context = True,
    python_callable = load_ods2edw,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

tm_member_his_updated_1st_purchase_date = PythonOperator(
    task_id='tm_member_his_updated_1st_purchase_date',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"tm_member_his" , 'sql_section': 'updated_by_1st_purchase_date', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

postprocess_tm_member_1st_purchase_his_task = PythonOperator(
    task_id = 'postprocess_tm_member_1st_purchase_his_task',
    provide_context = True,
    python_callable = post_process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

preprocess_tm_member_1st_purchase_his_task >> tm_member_1st_purchase_his_src2stg_task >> tm_member_1st_purchase_his_stg2ods_task >> tm_member_1st_purchase_his_ods2edw_task >> tm_member_his_updated_1st_purchase_date >>  postprocess_tm_member_1st_purchase_his_task