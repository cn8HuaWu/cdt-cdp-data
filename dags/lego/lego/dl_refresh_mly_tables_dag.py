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
from utils.myutil import Myutil
from utils.db import Mydb
from subdags.subdag_sync_rds import sync_subdag
from update_downstream_table import update_downstream

# variable to run the shell scripts
SRC_NAME = "DL"
entity = "mly_sales_rpt"

myutil = Myutil(DAG_HOME)
gp_host = myutil.get_conf( 'Greenplum', 'GP_HOST')
gp_port = myutil.get_conf( 'Greenplum', 'GP_PORT')
gp_db = myutil.get_conf( 'Greenplum', 'GP_DB')
gp_usr = myutil.get_conf( 'Greenplum', 'GP_USER')
gp_pw = myutil.get_conf( 'Greenplum', 'GP_PASSWORD')
db = Mydb(gp_host, gp_port, gp_db, gp_usr, gp_pw)
entity_conf = myutil.get_entity_config()
email_to_list =  Variable.get('email_to_list').split(',')
DAG_NAME = 'dl_refresh_mly_tables_dag'

batch_date = datetime.strftime(datetime.now(),'%Y%m%d')
dag_start_date = Variable.get('dag_start_date').strip()
scv_interval = Variable.get('interval_refresh_monthly_tables').strip()

args = {
    'owner': 'cdp_admin',
    'email': email_to_list,
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': datetime.strptime(dag_start_date,'%Y-%m-%d %H:%M:%S'),
    'max_active_runs': 1,
    "src_name": SRC_NAME,
    "batch_date_args": 'preprocess_refresh_mly_task'
}

def process_fileload(is_encrypted = False, is_compressed = False, **kwargs):
    kwargs['task_instance'].xcom_push(key='batch_date', value=batch_date)



dag = DAG(dag_id = DAG_NAME,
            default_args = args,
            concurrency = 1, 
            max_active_runs = 1, 
            schedule_interval = scv_interval)

preprocess_refresh_mly_task = PythonOperator(
    task_id = 'preprocess_refresh_mly_task',
    provide_context = True,
    python_callable = process_fileload,
    retries = 1,
    op_kwargs = {'is_encrypted': False},
    dag = dag,
)

updated_by_jd_order_dtl_task = PythonOperator(
    task_id='updated_by_jd_order_dtl_task',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_mly_sales_rpt" , 'sql_section': 'update_by_jd_order_dtl', 'args': args},
    dag=dag,
)

updated_by_jd_member = PythonOperator(
    task_id='updated_by_jd_member',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_mly_sales_rpt" , 'sql_section': 'update_by_jd_member', 'args': args},
    dag=dag,
)

updated_by_jd_and_tm_traffic = PythonOperator(
    task_id='updated_by_jd_and_tm_traffic',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_mly_sales_rpt" , 'sql_section': 'update_by_jd_and_tm_traffic', 'args': args},
    dag=dag,
)

updated_by_oms_order_dtl = PythonOperator(
    task_id='updated_by_oms_order_dtl',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_mly_sales_rpt" , 'sql_section': 'update_by_oms_order_dtl', 'args': args},
    dag=dag,
)
scv_sync_2_rds_task = SubDagOperator(
    task_id='scv_sync_2_rds_task',
    subdag=sync_subdag(DAG_NAME, 'scv_sync_2_rds_task', myutil, entity_conf, args, entity),
    default_args=args,
    executor=SequentialExecutor(),
    dag=dag,
)


preprocess_refresh_mly_task >> updated_by_jd_order_dtl_task >> updated_by_jd_member >>  updated_by_jd_and_tm_traffic >> updated_by_oms_order_dtl >> scv_sync_2_rds_task 
