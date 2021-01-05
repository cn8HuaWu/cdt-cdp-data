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

DAG_HOME =  Variable.get('dag_home').strip().rstrip('/')
sys.path.append("../tasks/")
sys.path.append(DAG_HOME + "/tasks/")

from src2stg import Src2stgHandler
from stg2ods import Stg2odsHandler
from ods2edw import Ods2edwHandler
from utils.myutil import Myutil
from utils.db import Mydb
from subdags.subdag_sync_rds import sync_subdag
from update_downstream_table import update_downstream
from pytz import timezone

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
entity = 'member'
src_entity = 'tm_member'
DAG_NAME = 'tm_member_dag'

has_head = 0
batch_date = datetime.strftime( datetime.now(timezone('Asia/Shanghai')), '%Y%m%d' )

def post_process_fileload( **kwargs):
    pass

def dag_failure_handler(context):
    pass


def load_ods2edw(**kwargs):
    pkey = entity_conf[src_entity]["key"]
    table_prefix = entity_conf[src_entity]["edw_prefix"]
    update_type = entity_conf[src_entity]["update_type"]
    #
    my_batch_date = batch_date
    kwargs['task_instance'].xcom_push(key='batch_date', value=batch_date)
    ods2edw = Ods2edwHandler(  my_batch_date, 
        SRC_NAME, 
        entity, 
        pkey,
        table_prefix, 
        myutil, 
        db)
    ods2edw.start()

args = {
    'owner': 'cdp_admin',
    'email': email_to_list,
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': days_ago(0),
    'max_active_runs': 1,
    'retries': 0,
    "src_name": SRC_NAME,
    "batch_date_args": 'tm_member_ods2edw_task'
}

dag = DAG(dag_id = DAG_NAME,
            default_args = args,
            concurrency = 3, 
            max_active_runs = 1, 
            schedule_interval = None)

tm_member_ods2edw_task = PythonOperator(
    task_id='tm_member_ods2edw_task',
    provide_context = True,
    python_callable = load_ods2edw,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

tm_member_update_oms_order_dtl = PythonOperator(
    task_id='tm_member_update_oms_order_dtl',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"oms_order_dtl" , 'sql_section': 'update_by_jd_member', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

tm_member_update_dly_sales_rpt = PythonOperator(
    task_id='tm_member_update_dly_sales_rpt',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_dly_sales_rpt" , 'sql_section': 'update_by_oms_order_dtl', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

tm_member_update_dly_sales_rpt_member = PythonOperator(
    task_id='tm_member_update_dly_sales_rpt_member',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_dly_sales_rpt" , 'sql_section': 'update_by_tm_member', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

tm_member_sync_2_rds_task = SubDagOperator(
    task_id='tm_member_sync_2_rds_task',
    subdag=sync_subdag(DAG_NAME, 'tm_member_sync_2_rds_task', myutil, entity_conf, args, entity),
    default_args=args,
    executor=SequentialExecutor(),
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

tm_member_ods2edw_task >> tm_member_update_oms_order_dtl >> tm_member_update_dly_sales_rpt >> tm_member_update_dly_sales_rpt_member>> tm_member_sync_2_rds_task