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
SRC_NAME = "JD"
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

entity = 'pop_order_dtl_his'
src_entity = 'jd_pop_order_dtl_his'
batch_date = datetime.strftime(datetime.now(), '%Y%m%d')
DAG_NAME = 'jd_pop_order_dtl_his_dag'
history_path = Variable.get('pop_historical_path')
sql_file_name = SRC_NAME.lower()+"_"+entity

def process_fileload(is_encrypted = False, is_compressed = False, **kwargs):
    src_file_list = myutil.get_sql_yml_fd( sql_file_name )["Staging"]["src_file_list"].split(',')
    bucket = myutil.get_oss_bucket()
    stg_path = '/'.join((STAGING, SRC_NAME, entity, batch_date))
    timestamp_str =  datetime.now().strftime('%Y%m%d_%H%M%S_%f')

    encrypt_col_list = entity_conf[src_entity]['to_encrypt_columns'].split(",")
    sql_dict =  myutil.get_sql_yml_fd(src_entity)
    columns_list = sql_dict['Staging']['src_columns'].replace(' text','').replace(' ','').split(",")
    dl_aes_key = myutil.get_dl_aes_key()
    dl_aes_iv = myutil.get_dl_aes_iv()
    for fd in src_file_list:
        fname = fd.strip()
        localfile_dir = os.path.join(history_path, fname)
        # backup the history file
        
        backup_path ='/'.join( ('Backup', SRC_NAME, entity, timestamp_str + '_' + fd) )
        logging.info("Buck hsitory file to %s:", backup_path)
        myutil.upload_local_oss(bucket, localfile_dir, backup_path)
        
        # encrypt the fields: (cosignee_contact, taxpayer_phone)
        encrypted_path = localfile_dir + "_encrypted_" + timestamp_str
        myutil.encrypt_csv_fields(localfile_dir,
                encrypted_path,
                None, 
                encrypt_col_list, 
                columns_list, 
                dl_aes_key, 
                dl_aes_iv, 
                del_src= False, 
                algo = 'AES-256-CBC',
                keepheader = False )
        logging.info("Upload the encrypted hsitory file to %s:", stg_path + "/" + fname)
        myutil.upload_local_oss(bucket, encrypted_path, stg_path + "/" + fname)
        os.remove(encrypted_path)
        # upload the stg level as the GP external table
        # myutil.upload_local_oss(bucket, encrypted_path, stg_path + "/" + fname)
    

def post_process_fileload( **kwargs):
    #rename: change prefix to "done-"
    pass

def dag_failure_handler(context):
    #rename: change prefix to "failed-"
    pass

def load_stg2ods(**kwargs):
    pkey = entity_conf[src_entity]["key"]
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    stg2ods = Stg2odsHandler(TEMP_FOLDER, STAGING, ODS, batch_date, SRC_NAME, entity, stg_suffix, pkey, myutil, db, has_head=0 )
    stg2ods.start()


def load_ods2edw(**kwargs):
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
    "src_name": SRC_NAME
}

dag = DAG(dag_id = DAG_NAME,
            default_args = args,
            concurrency = 1, 
            max_active_runs = 1, 
            schedule_interval = None)

preprocess_pop_order_dtl_his_task = PythonOperator(
    task_id = 'preprocess_pop_order_dtl_his_task',
    provide_context = True,
    python_callable = process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

pop_order_dtl_his_stg2ods_task = PythonOperator(
    task_id='pop_order_dtl_his_stg2ods_task',
    provide_context = True,
    python_callable = load_stg2ods,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

pop_order_dtl_his_ods2edw_task = PythonOperator(
    task_id='pop_order_dtl_his_ods2edw_task',
    provide_context = True,
    python_callable = load_ods2edw,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)


postprocess_pop_order_dtl_his_task = PythonOperator(
    task_id = 'postprocess_pop_order_dtl_his__task',
    provide_context = True,
    python_callable = post_process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

preprocess_pop_order_dtl_his_task >> pop_order_dtl_his_stg2ods_task >> pop_order_dtl_his_ods2edw_task  >>  postprocess_pop_order_dtl_his_task