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
DAG_NAME = 'dl_product_info_v2_daily_update_dag'
email_to_list =  Variable.get('email_to_list').split(',')

myutil = Myutil(dag_home=DAG_HOME, entity_name=src_entity)
db = myutil.get_db()
entity_conf = myutil.get_entity_config()

product_interval = Variable.get('interval_product').strip()
dag_start_date = Variable.get('dag_start_date').strip()
# product_batchdate = datetime.strftime(datetime.now(),'%Y%m%d')
update_attributions = ['update_product_status', 'daily_update_product_status']

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
            schedule_interval = product_interval,
)

def run_composit_task(query_sections:list, **kwargs):
    sql_dict = myutil.get_sql_yml_fd(src_entity)
    # batch_date = kwargs.get('dag_run').conf.get('batch_date')
    with db.create_session() as session:
        for set in query_sections:
            query = (sql_dict['EDW'][set])
            session.execute(query)

product_info_v2_daily_update_task = PythonOperator(
    task_id='product_info_v2_daily_update_task',
    provide_context = True,
    python_callable = run_composit_task,
    op_kwargs = {'query_sections': update_attributions},
    # on_failure_callback = dag_failure_handler,
    dag=dag,
)

product_info_v2_sync_2_rds_task = SubDagOperator(
    task_id='product_info_v2_sync_2_rds_task',
    subdag=sync_subdag(DAG_NAME, 'product_info_v2_sync_2_rds_task', myutil, entity_conf, args, entity),
    default_args=args,
    executor=SequentialExecutor(),
    # on_failure_callback = dag_failure_handler,
    dag=dag,
)

product_info_v2_daily_update_task >> product_info_v2_sync_2_rds_task