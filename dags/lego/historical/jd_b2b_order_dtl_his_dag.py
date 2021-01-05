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
entity = 'b2b_order_dtl_his'
src_entity = 'jd_b2b_order_dtl_his'
dly_entity = 'b2b_order_dtl'
dly_src_entity = 'jd_b2b_order_dtl'

DAG_NAME = 'jd_b2b_order_dtl_his_dag'
batch_date = datetime.strftime(datetime.now(), '%Y%m%d')

def process_fileload(is_encrypted = False, is_compressed = False, **kwargs):
    # OK file trigger
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
    
def load_n_files(**kwargs):
    history_path = Variable.get('b2b_historical_path')
    data_file_list = Variable.get('b2b_historical_file_list').split(",")

    bucket = myutil.get_oss_bucket()
    kwargs['task_instance'].xcom_push(key='batch_date', value=batch_date)

    stg_path = '/'.join((STAGING, SRC_NAME, entity, batch_date))
    timestamp_str =  datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    myutil.delete_oss_file_with_prefix( bucket, stg_path + "/" )

    for fd in data_file_list:
        # convert excel to csv
        input_file_path = os.path.join(history_path, fd)

        # backup the history file
        backup_path ='/'.join( ('Backup', SRC_NAME, entity, timestamp_str + '_' + fd) )
        myutil.upload_local_oss(bucket, input_file_path, backup_path)

        filepath,fullflname =  os.path.split(fd)
        fname,ext = os.path.splitext(fullflname)
        output_path = input_file_path
        if ( ext.lower() in ('.xlsx', '.xls')):
            fname = fname + ".csv"
            output_path = os.path.join(filepath, fname)
            myutil.read_excel_file(input_file_path, output_path, keephead=False) 
        elif (ext.lower() == '.csv'):
            fname = os.path.basename(fd)
        else:
            raise ValueError("The product info must be excel or csv")

        myutil.upload_local_oss(bucket, output_path, stg_path + "/" + fname)
        os.remove(output_path)


def post_process_fileload( **kwargs):
    #rename: change prefix to "done-"
    if ("skip_load"  in kwargs.get('dag_run').conf 
        and kwargs.get('dag_run').conf.get("skip_load").upper() == 'Y' ):
        return 

    if ("ok_file_path" in kwargs.get('dag_run').conf  ):
        OK_FILE_PATH  = kwargs.get('dag_run').conf.get('ok_file_path')
        myutil.modify_ok_file_prefix("running", "done", OK_FILE_PATH)

def dag_failure_handler(context):
    #rename: change prefix to "failed-"
    if ("ok_file_path" in context.get('dag_run').conf  ):
        OK_FILE_PATH  = context.get('dag_run').conf.get('ok_file_path')
        myutil.modify_ok_file_prefix("running", "failed",OK_FILE_PATH)


def load_src2stg(**kwargs):
    # my_batch_date = kwargs['task_instance'].xcom_pull(key='batch_date', task_ids='load_n_files_task')
    v_batch_date = kwargs.get('dag_run').conf.get('batch_date') if 'batch_date' in kwargs.get('dag_run').conf else batch_date
    src_filename = kwargs.get('dag_run').conf.get('src_filename')
    #
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    #
    OK_FILE_PATH  = kwargs.get('dag_run').conf.get('ok_file_path')
    src2stg = Src2stgHandler(STAGING, v_batch_date, SRC_NAME, entity, stg_suffix, src_filename, myutil, OK_FILE_PATH, excel_skip_row = 0)
    src2stg.start()

def load_stg2ods(**kwargs):
    my_batch_date = kwargs['task_instance'].xcom_pull(key='batch_date', task_ids='load_n_files_task')
    v_batch_date = kwargs.get('dag_run').conf.get('batch_date') if 'batch_date' in kwargs.get('dag_run').conf else my_batch_date
    #
    pkey = entity_conf[src_entity]["key"]
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    #
    stg2ods = Stg2odsHandler(TEMP_FOLDER, STAGING, ODS, v_batch_date, SRC_NAME, entity, stg_suffix, pkey, myutil, db )
    stg2ods.start()

def load_ods2edw(**kwargs):
    my_batch_date = kwargs['task_instance'].xcom_pull(key='batch_date', task_ids='load_n_files_task')
    v_batch_date =kwargs.get('dag_run').conf.get('batch_date') if 'batch_date' in kwargs.get('dag_run').conf else my_batch_date
    #
    pkey = entity_conf[src_entity]["key"]
    table_prefix = entity_conf[src_entity]["edw_prefix"]
    update_type = entity_conf[src_entity]["update_type"]
    #
    ods2edw = Ods2edwHandler(  v_batch_date, SRC_NAME, entity, pkey,table_prefix, myutil, db )
    ods2edw.start()

def load_b2b_dtl_ods2edw(**kwargs):
    my_batch_date = kwargs['task_instance'].xcom_pull(key='batch_date', task_ids='load_n_files_task')
    v_batch_date = kwargs.get('dag_run').conf.get('batch_date') if 'batch_date' in kwargs.get('dag_run').conf else my_batch_date
    #
    pkey = entity_conf[dly_src_entity]["key"]
    table_prefix = entity_conf[dly_src_entity]["edw_prefix"]
    update_type = entity_conf[dly_src_entity]["update_type"]
    #
    ods2edw = Ods2edwHandler(  v_batch_date, SRC_NAME, dly_entity, pkey,table_prefix, myutil, db )
    ods2edw.start()

def b_choice(**kwargs):
    if ("skip_load"  in kwargs.get('dag_run').conf 
        and kwargs.get('dag_run').conf.get("skip_load").upper() == 'Y' ):
        return "b2b_order_dtl_his_dummy"
    else:
        return "branch_1orN_historical_file"

def historical_choice(**kwargs):
    if ("ok_file_path" in kwargs.get('dag_run').conf  ):
        return "preprocess_b2b_order_dtl_his_task"
    else:
        return "load_n_files_task"

args = {
    'owner': 'cdp_admin',
    'email': email_to_list,
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': days_ago(0),
    'max_active_runs': 1,
    "src_name": SRC_NAME,
    "batch_date_args": 'load_n_files_task',
}

dag = DAG(dag_id = DAG_NAME,
            default_args = args,
            concurrency = 5, 
            max_active_runs = 1, 
            schedule_interval = None)


branch_skip_load_edw = BranchPythonOperator(
    task_id='branch_skip_load_edw',
    python_callable= b_choice,
    provide_context = True,
    dag=dag,
)

branch_1orN_historical_file = BranchPythonOperator(
    task_id='branch_1orN_historical_file',
    python_callable= historical_choice,
    provide_context = True,
    dag=dag,
)

b2b_order_dtl_his_dummy = DummyOperator(
        task_id="b2b_order_dtl_his_dummy",
        dag=dag,
    )

preprocess_b2b_order_dtl_his_task = PythonOperator(
    task_id = 'preprocess_b2b_order_dtl_his_task',
    provide_context = True,
    python_callable = process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

load_n_files_task = PythonOperator(
    task_id = 'load_n_files_task',
    provide_context = True,
    python_callable = load_n_files,
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

branch_skip_load_edw >> branch_1orN_historical_file >> preprocess_b2b_order_dtl_his_task


b2b_order_dtl_his_src2stg_task = PythonOperator(
    task_id='b2b_order_dtl_his_src2stg_task',
    provide_context = True,
    python_callable = load_src2stg,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

b2b_order_dtl_his_stg2ods_task = PythonOperator(
    task_id='b2b_order_dtl_his_stg2ods_task',
    provide_context = True,
    python_callable = load_stg2ods,
    on_failure_callback = dag_failure_handler,
    trigger_rule='one_success',
    dag=dag,
)

branch_1orN_historical_file >> load_n_files_task >> b2b_order_dtl_his_stg2ods_task

b2b_order_dtl_his_ods2edw_task = PythonOperator(
    task_id='b2b_order_dtl_his_ods2edw_task',
    provide_context = True,
    python_callable = load_ods2edw,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

b2b_order_dtl_ods2edw_task = PythonOperator(
    task_id='b2b_order_dtl_ods2edw_task',
    provide_context = True,
    python_callable = load_b2b_dtl_ods2edw,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

# b2b_order_dtl_his_post_update = PythonOperator(
#     task_id='b2b_order_dtl_his_post_update',
#     provide_context = True,
#     python_callable = update_downstream,
#     op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"jd_b2b_order_dtl_his" , 'sql_section': 'update_by_product_info', 'args': args},
#     on_failure_callback = dag_failure_handler,
#     dag=dag,
# )

b2b_order_dtl_his_updated_by_b2b_gwp_and_sku_map = PythonOperator(
    task_id='b2b_order_dtl_his_update_by_b2b_gwp_and_sku_map',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"jd_b2b_order_dtl" , 'sql_section': 'update_by_b2b_gwp_and_sku_map', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

b2b_order_dtl_his_updated_by_product_info = PythonOperator(
    task_id='b2b_order_dtl_his_update_by_product_info',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"jd_b2b_order_dtl" , 'sql_section': 'update_by_product_info', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

b2b_order_dtl_his_updated_by_jd_member = PythonOperator(
    task_id='b2b_order_dtl_his_update_by_jd_member',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"jd_b2b_order_dtl" , 'sql_section': 'update_by_jd_member', 'args': args},
    on_failure_callback = dag_failure_handler,
    trigger_rule='none_failed',
    dag=dag,
)

b2b_order_dtl_his_update_b2b_order = PythonOperator(
    task_id='b2b_order_dtl_his_update_b2b_order',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"jd_b2b_order" , 'sql_section': 'update_by_b2b_order_dtl', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

b2b_order_dtl_his_update_jd_member = PythonOperator(
    task_id='b2b_order_dtl_his_update_jd_member',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"jd_member" , 'sql_section': 'update_by_pop_order_and_b2b_order_dtl', 'args': args},
    trigger_rule='none_failed',
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

branch_skip_load_edw >> b2b_order_dtl_his_dummy >> b2b_order_dtl_his_update_jd_member

b2b_order_dtl_his_update_pop_order_dtl = PythonOperator(
    task_id='b2b_order_dtl_his_update_pop_order_dtl',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"jd_pop_order_dtl" , 'sql_section': 'update_by_jd_member', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

b2b_order_dtl_his_update_dl_order_dtl = PythonOperator(
    task_id='b2b_order_dtl_his_update_dl_order_dtl',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_jd_order_dtl", 'sql_section': 'update_by_jd_order_dtl', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

b2b_order_dtl_his_update_dl_order = PythonOperator(
    task_id='b2b_order_dtl_his_update_dl_order',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_jd_order" , 'sql_section': 'update_by_jd_order', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

b2b_order_dtl_his_update_dl_shopper = PythonOperator(
    task_id='b2b_order_dtl_his_update_dl_shopper',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_jd_shopper" , 'sql_section': 'update_by_jd_order', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

b2b_order_dtl_his_update_dly_sales_rpt = PythonOperator(
    task_id='b2b_order_dtl_his_update_dly_sales_rpt',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_dly_sales_rpt" , 'sql_section': 'update_by_jd_order_dtl', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

# b2b_order_dtl_his_update_mly_sales_rpt = PythonOperator(
#     task_id='b2b_order_dtl_his_update_mly_sales_rpt',
#     provide_context = True,
#     python_callable = update_downstream,
#     op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_mly_sales_rpt" , 'sql_section': 'update_by_jd_order_dtl', 'args': args},
#     on_failure_callback = dag_failure_handler,
#     dag=dag,
# )

b2b_order_dtl_his_sync_2_rds_task = SubDagOperator(
    task_id='b2b_order_dtl_his_sync_2_rds_task',
    subdag=sync_subdag(DAG_NAME, 'b2b_order_dtl_his_sync_2_rds_task', myutil, entity_conf, args, dly_entity),
    default_args=args,
    executor=SequentialExecutor(),
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

postprocess_b2b_order_dtl_his_task = PythonOperator(
    task_id = 'postprocess_b2b_order_dtl_his_task',
    provide_context = True,
    python_callable = post_process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    dag = dag,
)

# have issue to update in currency
preprocess_b2b_order_dtl_his_task >> b2b_order_dtl_his_src2stg_task >> b2b_order_dtl_his_stg2ods_task >>  b2b_order_dtl_his_ods2edw_task >> b2b_order_dtl_ods2edw_task
b2b_order_dtl_ods2edw_task >> b2b_order_dtl_his_update_jd_member >> b2b_order_dtl_his_updated_by_b2b_gwp_and_sku_map >> b2b_order_dtl_his_updated_by_product_info >> b2b_order_dtl_his_updated_by_jd_member 
b2b_order_dtl_his_updated_by_jd_member >> b2b_order_dtl_his_update_pop_order_dtl >> b2b_order_dtl_his_update_dl_order_dtl
b2b_order_dtl_his_update_dl_order_dtl >> b2b_order_dtl_his_update_dl_shopper >> b2b_order_dtl_his_sync_2_rds_task
b2b_order_dtl_his_update_dl_order_dtl >> b2b_order_dtl_his_update_dly_sales_rpt  >> b2b_order_dtl_his_sync_2_rds_task
# b2b_order_dtl_his_update_dl_order_dtl >> b2b_order_dtl_his_update_mly_sales_rpt  >> b2b_order_dtl_his_sync_2_rds_task
b2b_order_dtl_his_update_dl_order_dtl >> b2b_order_dtl_his_update_b2b_order >> b2b_order_dtl_his_update_dl_order >> b2b_order_dtl_his_sync_2_rds_task
b2b_order_dtl_his_sync_2_rds_task >> postprocess_b2b_order_dtl_his_task
