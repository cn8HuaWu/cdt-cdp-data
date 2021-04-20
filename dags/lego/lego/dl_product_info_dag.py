from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.executors.sequential_executor import SequentialExecutor
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow import configuration as conf
from airflow import DAG

import logging
import os, sys
from pathlib import Path
from datetime import datetime
import re

DAG_HOME =  Variable.get('dag_home').strip().rstrip('/')
sys.path.append("../tasks/")
sys.path.append(DAG_HOME + "/tasks/")

from stg2ods import Stg2odsHandler
from ods2edw import Ods2edwHandler
from utils.myutil import Myutil
from utils.db import Mydb
from subdags.subdag_sync_rds import sync_subdag
from update_downstream_table import update_downstream
from datetime import timedelta

# variable to run the shell scripts
SRC_NAME = "DL"
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

entity = 'product_info'
src_entity = 'dl_product_info'
DAG_NAME = 'dl_product_info_dag'
entity_conf = myutil.get_entity_config()
email_to_list =  Variable.get('email_to_list').split(',')

# _ver_year = Variable.get('product_version_year')
# _cur_year = datetime.now().year
# PRD_VERSION = _ver_year if (re.search('^\d{4}$',_ver_year) and abs(_cur_year - int(_ver_year) < 5  )) else str(_cur_year)
# batch_date = datetime.strftime(datetime.now(),'%Y%m%d')
dldy = timedelta(days=1)
product_batchdate = datetime.strftime(datetime.now()-dldy,'%Y%m%d')

# entity = 'product_info'
data_file_list = Variable.get('product_info_path').split(",")
product_interval = Variable.get('interval_product').strip()
dag_start_date = Variable.get('dag_start_date').strip()

def process_fileload(is_encrypted = False, is_compressed = False, **kwargs):
    my_batch_date = kwargs['task_instance'].xcom_pull(key='batch_date', task_ids='branch_external_trigger')
    bucket = myutil.get_oss_bucket()
    stg_path = '/'.join((STAGING, SRC_NAME, entity, my_batch_date))
    timestamp_str =  datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    for fd in data_file_list:
        # convert excel to csv
        output_path = fd
        filepath,fullflname =  os.path.split(fd)

        basename = os.path.basename(fd)
        backup_path ='/'.join( ('Backup', 'DL', entity, timestamp_str + '_' + basename) )
        myutil.upload_local_oss(bucket, fd, backup_path)

        fname,ext = os.path.splitext(fullflname)
        if ( ext.lower() in ('.xlsx', '.xls')):
            fname = fname + ".csv"
            output_path = os.path.join(filepath, fname)
            myutil.read_excel_file(fd, output_path, keephead=False) 
        elif (ext.lower() == '.csv'):
            fname = os.path.basename(fd)
        else:
            raise ValueError("The product info must be excel or csv")

        myutil.delete_oss_file_with_prefix( bucket, stg_path + "/" )

        myutil.upload_local_oss(bucket, output_path, stg_path + "/" + fname)

def post_process_fileload( **kwargs):
    #rename: change prefix to "done-"
    pass

def dag_failure_handler(context):
    #rename: change prefix to "failed-"
   pass

def load_stg2ods(**kwargs):
    pkey = entity_conf[src_entity]["key"]
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    my_batch_date = kwargs['task_instance'].xcom_pull(key='batch_date', task_ids='branch_external_trigger')
    stg2ods = Stg2odsHandler(TEMP_FOLDER, STAGING, ODS, my_batch_date, SRC_NAME, entity, stg_suffix, pkey, myutil, db, has_head=0)
    stg2ods.start()


def load_ods2edw(**kwargs):
    pkey = entity_conf[src_entity]["key"]
    table_prefix = entity_conf[src_entity]["edw_prefix"]
    update_type = entity_conf[src_entity]["update_type"]
    my_batch_date = kwargs['task_instance'].xcom_pull(key='batch_date', task_ids='branch_external_trigger')
    ods2edw = Ods2edwHandler(  my_batch_date, SRC_NAME, entity, pkey,table_prefix, myutil, db )
    ods2edw.start()

def b_choice(**kwargs):
    kwargs['task_instance'].xcom_push(key='batch_date', value=product_batchdate)
    logging.info("externally triggered:" + str(kwargs.get('dag_run').external_trigger))
    if (kwargs.get('dag_run').external_trigger ):
        if ("skip_load"  in kwargs.get('dag_run').conf and kwargs.get('dag_run').conf.get("skip_load").upper() == 'Y' ):
             return "product_info_dummy"
        else:
            return "preprocess_product_info_task"
    else:
        return "product_info_dummy"

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
    "batch_date_args": 'branch_external_trigger'
}

dag = DAG(dag_id = DAG_NAME,
            default_args = args,
            concurrency = 5, 
            max_active_runs = 1, 
            schedule_interval = product_interval,
)

branch_external_trigger = BranchPythonOperator(
    task_id='branch_external_trigger',
    python_callable= b_choice,
    provide_context = True,
    dag=dag,
)

product_info_dummy = DummyOperator(
    task_id="product_info_dummy",
    dag=dag,
)

preprocess_product_info_task = PythonOperator(
    task_id = 'preprocess_product_info_task',
    provide_context = True,
    python_callable = process_fileload,
    retries = 1,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)


product_info_stg2ods_task = PythonOperator(
    task_id='product_info_stg2ods_task',
    provide_context = True,
    python_callable = load_stg2ods,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

product_info_ods2edw_task = PythonOperator(
    task_id='product_info_ods2edw_task',
    provide_context = True,
    python_callable = load_ods2edw,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

product_update_product_status_daily = PythonOperator(
    task_id='product_update_product_status_daily',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_product_info" , 'sql_section': 'daily_update_product_status', 'args': args},
    on_failure_callback = dag_failure_handler,
    trigger_rule='none_failed',
    dag=dag,
)

# product_updated_by_b2b_sku_map = PythonOperator(
#     task_id='product_updated_by_b2b_sku_map',
#     provide_context = True,
#     python_callable = update_downstream,
#     op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_product_info" , 'sql_section': 'update_by_b2b_sku_map', 'args': args},
#     on_failure_callback = dag_failure_handler,
#     dag=dag,
# )

# product_updated_by_tm_sku_map = PythonOperator(
#     task_id='product_updated_by_tm_sku_map',
#     provide_context = True,
#     python_callable = update_downstream,
#     op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_product_info" , 'sql_section': 'update_by_tm_sku_map', 'args': args},
#     on_failure_callback = dag_failure_handler,
#     dag=dag,
# )

# product_updated_by_pop_sku_map = PythonOperator(
#     task_id='product_updated_by_pop_sku_map',
#     provide_context = True,
#     python_callable = update_downstream,
#     op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_product_info" , 'sql_section': 'update_by_pop_sku_map', 'args': args},
#     on_failure_callback = dag_failure_handler,
#     dag=dag,
# )

product_update_b2b_order_dtl = PythonOperator(
    task_id='product_update_b2b_order_dtl',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"jd_b2b_order_dtl" , 'sql_section': 'update_by_product_info', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)


product_update_b2b_order = PythonOperator(
    task_id='product_update_b2b_order',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"jd_b2b_order" , 'sql_section': 'update_by_b2b_order_dtl', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

product_update_pop_order_dtl = PythonOperator(
    task_id='product_update_pop_order_dtl',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"jd_pop_order_dtl" , 'sql_section': 'update_by_product_info', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

product_update_pop_order = PythonOperator(
    task_id='pop_update_pop_order',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"jd_pop_order" , 'sql_section': 'update_by_pop_order_dtl', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

product_update_oms_order_dtl = PythonOperator(
    task_id='product_update_oms_order_dtl',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"oms_order_dtl" , 'sql_section': 'update_by_product_info', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

product_update_dl_order_dtl = PythonOperator(
    task_id='product_update_dl_order_dtl',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_jd_order_dtl", 'sql_section': 'update_by_jd_order_dtl', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

product_update_dl_order = PythonOperator(
    task_id='product_update_dl_order',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_jd_order" , 'sql_section': 'update_by_jd_order', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

product_update_dly_sales_rpt = PythonOperator(
    task_id='product_update_dly_sales_rpt',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_dly_sales_rpt" , 'sql_section': 'update_by_jd_order_dtl', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

product_update_dly_sales_rpt_oms = PythonOperator(
    task_id='product_update_dly_sales_rpt_oms',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_dly_sales_rpt" , 'sql_section': 'update_by_oms_order_dtl', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

# product_update_mly_sales_rpt_oms = PythonOperator(
#     task_id='product_update_mly_sales_rpt_oms',
#     provide_context = True,
#     python_callable = update_downstream,
#     op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_mly_sales_rpt" , 'sql_section': 'update_by_oms_order_dtl', 'args': args},
#     on_failure_callback = dag_failure_handler,
#     dag=dag,
# )


# product_update_mly_sales_rpt = PythonOperator(
#     task_id='product_update_mly_sales_rpt',
#     provide_context = True,
#     python_callable = update_downstream,
#     op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_mly_sales_rpt" , 'sql_section': 'update_by_jd_order_dtl', 'args': args},
#     on_failure_callback = dag_failure_handler,
#     dag=dag,
# )

# product_update_dl_tm_shopper = PythonOperator(
#     task_id='product_update_dl_tm_shopper',
#     provide_context = True,
#     python_callable = update_downstream,
#     op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_tm_shopper" , 'sql_section': 'update_by_oms_order_dtl', 'args': args},
#     on_failure_callback = dag_failure_handler,
#     dag=dag,
# )

product_update_dl_shopper = PythonOperator(
    task_id='product_update_dl_shopper',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_jd_shopper" , 'sql_section': 'update_by_jd_order', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

product_info_sync_2_rds_task = SubDagOperator(
    task_id='product_info_sync_2_rds_task',
    subdag=sync_subdag(DAG_NAME, 'product_info_sync_2_rds_task', myutil, entity_conf, args, entity),
    default_args=args,
    executor=SequentialExecutor(),
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

postprocess_product_info_task = PythonOperator(
    task_id = 'postprocess_product_info_task',
    provide_context = True,
    python_callable = post_process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

branch_external_trigger >> product_info_dummy >> product_update_product_status_daily
branch_external_trigger >> preprocess_product_info_task
preprocess_product_info_task >> product_info_stg2ods_task >> product_info_ods2edw_task
# product_info_ods2edw_task >> product_updated_by_pop_sku_map >> product_updated_by_tm_sku_map >> product_updated_by_b2b_sku_map >> product_update_product_status_daily

product_info_ods2edw_task  >> product_update_product_status_daily
product_update_product_status_daily >> product_update_b2b_order_dtl >> product_update_dl_order_dtl
product_update_dl_order_dtl >> product_update_dly_sales_rpt >> product_update_dly_sales_rpt_oms >> product_info_sync_2_rds_task  
# product_update_dl_order_dtl >> product_update_mly_sales_rpt >> product_update_mly_sales_rpt_oms >> product_info_sync_2_rds_task 
product_update_b2b_order_dtl >> product_update_b2b_order >> product_update_dl_order >> product_info_sync_2_rds_task

product_update_product_status_daily >> product_update_pop_order_dtl >> product_update_dl_order_dtl
# product_update_dl_order_dtl >> product_update_dly_sales_rpt >>product_info_sync_2_rds_task  
# product_update_dl_order_dtl >> product_update_mly_sales_rpt >> product_info_sync_2_rds_task
product_update_pop_order_dtl >> product_update_pop_order >> product_update_dl_order >> product_info_sync_2_rds_task

product_update_product_status_daily >> product_update_oms_order_dtl >> product_update_dly_sales_rpt_oms 
# product_update_product_status_daily >> product_update_oms_order_dtl >> product_update_mly_sales_rpt_oms
# product_update_product_status_daily >> product_update_oms_order_dtl >> product_update_dl_tm_shopper >> product_info_sync_2_rds_task
product_update_product_status_daily >> product_update_oms_order_dtl >> product_info_sync_2_rds_task    

product_update_pop_order_dtl >> product_update_dl_shopper >> product_info_sync_2_rds_task  
product_update_b2b_order_dtl >> product_update_dl_shopper >> product_info_sync_2_rds_task  
# product_info_ods2edw_task >> tm_sku_map_update_oms_order_dtl >> product_update_dly_sales_rpt >> product_info_sync_2_rds_task
# tm_sku_map_update_oms_order_dtl >> product_update_mly_sales_rpt >> product_info_sync_2_rds_task
# tm_sku_map_update_oms_order_dtl >> tm_sku_map_update_oms_order >> product_info_sync_2_rds_task

product_info_sync_2_rds_task  >> postprocess_product_info_task
