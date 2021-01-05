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
from datetime import datetime,timedelta
from pytz import timezone

DAG_HOME =  Variable.get('dag_home').strip().rstrip('/')
sys.path.append("../tasks/")
sys.path.append(DAG_HOME + "/tasks/")

from src2stg import Src2stgHandler
from stg2ods import Stg2odsHandler
from ods2edw import Ods2edwHandler
from utils.myutil import Myutil
from utils.db import Mydb
from subdags.subdag_sync_rds import sync_subdag
from subdags.subdag_post_update_edw import update_subdag

# variable to run the shell scripts
SRC_NAME = "OMS"
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
entity = 'return_order_dtl'
src_entity = 'oms_return_order_dtl'
DAG_NAME = 'oms_return_order_dtl_dag'
has_head = 0
data_timedelta = int(Variable.get('oms_data_timedelta').strip())
batch_date = datetime.strftime( datetime.now(timezone('Asia/Shanghai')) - timedelta(days = data_timedelta ) , '%Y%m%d')

api_interval = Variable.get('interval_oms_return_order_dtl').strip()
dag_start_date = Variable.get('dag_start_date').strip()


def post_process_fileload( **kwargs):
    pass

def dag_failure_handler(context):
    pass

def load_stg2ods(**kwargs):
    pkey = entity_conf[src_entity]["key"]
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    #
    kwargs['task_instance'].xcom_push(key='batch_date', value=batch_date)
    stg2ods = Stg2odsHandler(TEMP_FOLDER, STAGING, ODS, batch_date, SRC_NAME, entity, stg_suffix, pkey, myutil, db, has_head = has_head )
    stg2ods.start()

def load_ods2edw(**kwargs):
    pkey = entity_conf[src_entity]["key"]
    table_prefix = entity_conf[src_entity]["edw_prefix"]
    update_type = entity_conf[src_entity]["update_type"]
    #
    my_batch_date = kwargs['task_instance'].xcom_pull(key='batch_date', task_ids='oms_return_order_dtl_stg2ods_task')
    ods2edw = Ods2edwHandler(  batch_date, SRC_NAME, entity, pkey,table_prefix, myutil, db )
    ods2edw.start()

args = {
    'owner': 'cdp_admin',
    'email': email_to_list,
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': datetime.strptime(dag_start_date,'%Y-%m-%d %H:%M:%S'),
    'max_active_runs': 1,
    'retries': 0,
    "src_name": SRC_NAME,
    "batch_date_args": 'oms_return_order_dtl_stg2ods_task'
}

dag = DAG(dag_id = DAG_NAME,
            default_args = args,
            concurrency = 1, 
            max_active_runs = 1, 
            schedule_interval = api_interval,
        )

oms_return_order_dtl_stg2ods_task = PythonOperator(
    task_id='oms_return_order_dtl_stg2ods_task',
    provide_context = True,
    python_callable = load_stg2ods,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

oms_return_order_dtl_ods2edw_task = PythonOperator(
    task_id='oms_return_order_dtl_ods2edw_task',
    provide_context = True,
    python_callable = load_ods2edw,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

# oms_return_order_dtl_post_update_task = SubDagOperator(
#     task_id='oms_return_order_dtl_post_update_task',
#     subdag=update_subdag(DAG_NAME, 'oms_return_order_dtl_post_update_task', myutil, db,  args),
#     default_args=args,
#     executor=SequentialExecutor(),
#     on_failure_callback = dag_failure_handler,
#     dag=dag,
# )

postprocess_oms_return_order_dtl_task = PythonOperator(
    task_id = 'postprocess_oms_return_order_dtl_task',
    provide_context = True,
    python_callable = post_process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

oms_return_order_dtl_stg2ods_task >> oms_return_order_dtl_ods2edw_task >>  postprocess_oms_return_order_dtl_task