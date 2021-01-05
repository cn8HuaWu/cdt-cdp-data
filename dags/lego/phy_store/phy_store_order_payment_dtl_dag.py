from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow import configuration as conf
from airflow import DAG
from airflow.utils.db import provide_session
from airflow.models import XCom
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators import DatahubSensor

import logging
import os, sys
from pathlib import Path
from datetime import datetime,timedelta

DAG_HOME =  Variable.get('dag_home').strip().rstrip('/')
sys.path.append("../tasks/")
sys.path.append(DAG_HOME + "/tasks/")

from stg2ods import Stg2odsHandler
from ods2edw import Ods2edwHandler
from utils.myutil import Myutil
from utils.db import Mydb
from subdags.subdag_sync_rds import sync_subdag
from update_downstream_table import update_downstream
from pytz import timezone

# variable to run the shell scripts
SRC_NAME = "phy"
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
entity = 'store_order_payment_dtl'
src_entity = 'phy_store_order_payment_dtl'
DAG_NAME = 'phy_store_order_payment_dtl_dag'

has_head = 0

data_timedelta = int(Variable.get('phy_store_data_timedelta').strip())
batch_date = datetime.strftime( datetime.now(timezone('Asia/Shanghai')) - timedelta(days = data_timedelta ) , '%Y%m%d')
api_interval = Variable.get('interval_phy_store_order_payment_dtl').strip()
dag_start_date = Variable.get('dag_start_date').strip()

lego_lewin_mail_to = Variable.get('lego_lewin_mail_to').strip().split(",") 
lego_lewin_subject = Variable.get('lego_lewin_subject').strip() + "- LEWIN order payment detail" 
lego_lewin_html_content =  "LEWIN " + Variable.get('lego_lewin_html_content').strip() + "So far, No new coming data in phy_store_order_payment_dtl topic since last run"

@provide_session
def cleanup_xcom(context, session=None):
    session.query(XCom).filter(XCom.dag_id == DAG_NAME).delete()

def post_process_fileload( **kwargs):
    cleanup_xcom(kwargs)

    datahub_sql_dict = myutil.get_sql_yml_fd('datahub_oss_stg_check')
    engine = db.create_engine()
    conn = db.create_conn(engine)
    update_count_sql = datahub_sql_dict[src_entity+"_update_state"]
    update_count_sql = update_count_sql.replace("?", 'done')
    db.execute(update_count_sql, conn)

def dag_failure_handler(context):
    cleanup_xcom(context)

    datahub_sql_dict = myutil.get_sql_yml_fd('datahub_oss_stg_check')
    engine = db.create_engine()
    conn = db.create_conn(engine)
    update_count_sql = datahub_sql_dict[src_entity+"_update_state"]
    update_count_sql = update_count_sql.replace("?", 'failed')
    db.execute(update_count_sql, conn)

def has_new_data(**kwargs):
    updated_flag = kwargs['task_instance'].xcom_pull(key='new_data_flag', task_ids='phy_store_order_payment_dtl_sensor_task')
    if ( updated_flag in('', 'False') ):
        return "phy_store_order_payment_dtl_nodata_mail_task"
    else:
        return "phy_store_order_payment_dtl_stg2ods_task"

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
    my_batch_date = kwargs['task_instance'].xcom_pull(key='batch_date', task_ids='phy_store_order_payment_dtl_stg2ods_task')
    ods2edw = Ods2edwHandler(  my_batch_date, 
        SRC_NAME, 
        entity, 
        pkey,
        table_prefix, 
        myutil, 
        db, 
        # AES_KEY=aes_key, 
        # AES_ENCODING=aes_encoding,
        # DL_AES_KEY = dl_aes_key,
        # DL_AES_IV = dl_aes_iv,
        # has_param=True  
        )
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
    "batch_date_args": 'phy_store_order_payment_dtl_stg2ods_task',
}

dag = DAG(dag_id = DAG_NAME,
        default_args = args,
        concurrency = 3, 
        max_active_runs = 1, 
        catchup=False,
        schedule_interval = api_interval
)

phy_store_order_payment_dtl_sensor_task = DatahubSensor(
    mydb=db, 
    dh_id=src_entity, 
    myutil= myutil, 
    task_id='phy_store_order_payment_dtl_sensor_task', 
    poke_interval=100, 
    dag=dag
)

has_new_data_branch_task = BranchPythonOperator(
    task_id="has_new_data_branch_task",
    python_callable= has_new_data,
    provide_context = True,
    dag=dag,
)

phy_store_order_payment_dtl_nodata_mail_task = EmailOperator(
    task_id="phy_store_order_payment_dtl_nodata_mail_task",
    dag=dag,
    on_failure_callback = dag_failure_handler,
    to = lego_lewin_mail_to,
    subject = lego_lewin_subject,
    html_content = lego_lewin_html_content
)

phy_store_order_payment_dtl_stg2ods_task = PythonOperator(
    task_id='phy_store_order_payment_dtl_stg2ods_task',
    provide_context = True,
    python_callable = load_stg2ods,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

phy_store_order_payment_dtl_ods2edw_task = PythonOperator(
    task_id='phy_store_order_payment_dtl_ods2edw_task',
    provide_context = True,
    python_callable = load_ods2edw,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

phy_order_payment_dtl_updated_by_phy_store = PythonOperator(
    task_id='phy_order_payment_dtl_updated_by_phy_store',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"phy_store_order_payment_dtl" , 'sql_section': 'update_by_phy_store', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

postprocess_phy_store_order_payment_dtl_task = PythonOperator(
    task_id = 'postprocess_phy_store_order_payment_dtl_task',
    provide_context = True,
    python_callable = post_process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    trigger_rule='none_failed',
    dag = dag,
)

phy_store_order_payment_dtl_sensor_task >> has_new_data_branch_task >> phy_store_order_payment_dtl_stg2ods_task
has_new_data_branch_task >> phy_store_order_payment_dtl_nodata_mail_task >> postprocess_phy_store_order_payment_dtl_task
phy_store_order_payment_dtl_stg2ods_task >>  phy_store_order_payment_dtl_ods2edw_task >> phy_order_payment_dtl_updated_by_phy_store >> postprocess_phy_store_order_payment_dtl_task