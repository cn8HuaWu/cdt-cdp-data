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
SRC_NAME = "CS"
STAGING = 'Staging'
ODS = 'ODS'
TEMP_FOLDER='Temp'

myutil = Myutil(DAG_HOME)
gp_host = myutil.get_conf( 'Greenplum', 'GP_HOST')
gp_port = myutil.get_conf( 'Greenplum', 'GP_PORT')
gp_db = myutil.get_conf( 'Greenplum', 'GP_DB')
gp_usr = myutil.get_conf( 'Greenplum', 'GP_USER')
gp_pw = myutil.get_conf( 'Greenplum', 'GP_PASSWORD')
aes_key = myutil.get_conf('CALLCENTER', 'AES_KEY')
aes_encoding = myutil.get_conf('CALLCENTER','AES_ENCODING')

# dl_aes_key = myutil.get_conf('ETL', 'AES_KEY')
# dl_aes_iv = myutil.get_conf('ETL', 'AES_IV')
dl_aes_key = myutil.get_dl_aes_key()
dl_aes_iv = myutil.get_dl_aes_iv()
db = Mydb(gp_host, gp_port, gp_db, gp_usr, gp_pw)

email_to_list =  Variable.get('email_to_list').split(',')
entity = 'consumer_info'
src_entity = 'cs_consumer_info'
DAG_NAME = 'cs_consumer_info_dag'
dag_start_date = Variable.get('dag_start_date').strip()
cs_consumer_interval = Variable.get('interval_cs_consumer').strip()

has_head = 0
batch_date = datetime.strftime(datetime.now(), '%Y%m%d')
entity_conf = myutil.get_entity_config()



def post_process_fileload( **kwargs):
    #rename: change prefix to "done-"
    pass

def dag_failure_handler(context):
    #rename: change prefix to "failed-"
    pass

def load_stg2ods(**kwargs):
    pkey = entity_conf[src_entity]["key"]
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    stg2ods = Stg2odsHandler(TEMP_FOLDER, STAGING, ODS, batch_date, SRC_NAME, entity, stg_suffix, pkey, myutil, db, has_head = has_head )
    stg2ods.start()


def load_ods2edw(**kwargs):
    pkey = entity_conf[src_entity]["key"]
    table_prefix = entity_conf[src_entity]["edw_prefix"]
    update_type = entity_conf[src_entity]["update_type"]
    ods2edw = Ods2edwHandler(batch_date,
        SRC_NAME,
        entity,
        pkey,
        table_prefix,
        myutil,
        db,
        AES_KEY=aes_key,
        AES_ENCODING=aes_encoding,
        DL_AES_KEY = dl_aes_key,
        DL_AES_IV = dl_aes_iv,
        has_param=True )
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
            # schedule_interval = None,
            schedule_interval = cs_consumer_interval,
            start_date = datetime.strptime(dag_start_date,'%Y-%m-%d %H:%M:%S'),
        )

consumer_info_stg2ods_task = PythonOperator(
    task_id='consumer_info_stg2ods_task',
    provide_context = True,
    python_callable = load_stg2ods,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

consumer_info_ods2edw_task = PythonOperator(
    task_id='consumer_info_ods2edw_task',
    provide_context = True,
    python_callable = load_ods2edw,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

postprocess_consumer_info_task = PythonOperator(
    task_id = 'postprocess_consumer_info__task',
    provide_context = True,
    python_callable = post_process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

consumer_info_stg2ods_task >> consumer_info_ods2edw_task >>  postprocess_consumer_info_task
