from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow import DAG

import logging
import os, sys

DAG_HOME = Variable.get('dag_home').strip().rstrip('/')
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
SRC_NAME = "lgc"
STAGING = 'Staging'
ODS = 'ODS'
TEMP_FOLDER = 'Temp'

entity = 'modified_product'
src_entity = 'lgc_modified_product'
DAG_NAME = 'lgc_modified_product_dag'

sheet ={
"sheet1":{
    'start_column':0,
    'column_width':4
}
}

myutil = Myutil(dag_home=DAG_HOME, entity_name=src_entity)
db = myutil.get_db()
entity_conf = myutil.get_entity_config()
email_to_list = Variable.get('email_to_list').split(',')


def process_fileload(is_encrypted=False, is_compressed=False, **kwargs):
    logging.info("current path: " + os.getcwd())
    OK_FILE_PATH = kwargs.get('dag_run').conf.get('ok_file_path')

    # remove the ok file and get the source file
    if (OK_FILE_PATH is None or not os.path.exists(OK_FILE_PATH)):
        logging.error("OK_FILE_PATH: %s, ok file does not exist. ", OK_FILE_PATH)
        raise IOError("OK_FILE_PATH not found")
        # else:
    #     os.remove(OK_FILE_PATH)

    if (not os.path.isfile(OK_FILE_PATH[:-3])):
        logging.error("Source file does not exist. File path: %s", OK_FILE_PATH[:-3])
        ## source file does not exist, set the Job failed
        raise IOError("Source file not found")

    myutil.modify_ok_file_prefix(old_prefix=None, prefix="running", ok_file_path=OK_FILE_PATH)


def post_process_fileload(**kwargs):
    # rename: change prefix to "done-"
    if ("skip_load" in kwargs.get('dag_run').conf
            and kwargs.get('dag_run').conf.get("skip_load").upper() == 'Y'):
        return
    OK_FILE_PATH = kwargs.get('dag_run').conf.get('ok_file_path')
    myutil.modify_ok_file_prefix("running", "done", OK_FILE_PATH)


def dag_failure_handler(context):
    # rename: change prefix to "failed-"
    OK_FILE_PATH = context.get('dag_run').conf.get('ok_file_path')
    myutil.modify_ok_file_prefix("running", "failed", OK_FILE_PATH)


def load_src2stg(**kwargs):
    batch_date = kwargs.get('dag_run').conf.get('batch_date')
    src_filename = kwargs.get('dag_run').conf.get('src_filename')
    #
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    #
    OK_FILE_PATH = kwargs.get('dag_run').conf.get('ok_file_path')
    excel_fun_list = [myutil.rearrange_columns]
    src2stg = Src2stgHandler(STAGING, batch_date, SRC_NAME, entity, stg_suffix, src_filename, myutil, OK_FILE_PATH, excel_fun_list=excel_fun_list, 
                             has_head=False, sheetname='sheet1', merge=False,**sheet)
    src2stg.start(version='v2')


def load_stg2ods(**kwargs):
    pkey = entity_conf[src_entity]["key"]
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    #
    batch_date = kwargs.get('dag_run').conf.get('batch_date')
    stg2ods = Stg2odsHandler(TEMP_FOLDER, STAGING, ODS, batch_date, SRC_NAME, entity, stg_suffix, pkey, myutil, db,
                             has_head=False)
    stg2ods.start()


# def load_ods2edw(**kwargs):
#    batch_date = kwargs.get('dag_run').conf.get('batch_date')
#    pkey = entity_conf[src_entity]["key"]
#    table_prefix = entity_conf[src_entity]["edw_prefix"]
#    update_type = entity_conf[src_entity]["update_type"]
#    ods2edw = Ods2edwHandler(batch_date, SRC_NAME, entity, pkey, table_prefix, myutil, db)
#    ods2edw.start()

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

dag = DAG(dag_id=DAG_NAME,
          default_args=args,
          concurrency=5,
          max_active_runs=1,
          schedule_interval=None)

preprocess_modified_product_task = PythonOperator(
    task_id='preprocess_modified_product_task',
    provide_context=True,
    python_callable=process_fileload,
    op_kwargs={'is_encrypted': False},
    on_failure_callback=dag_failure_handler,
    dag=dag,
)

modified_product_src2stg_task = PythonOperator(
    task_id='modified_product_src2stg_task',
    provide_context=True,
    python_callable=load_src2stg,
    on_failure_callback=dag_failure_handler,
    dag=dag,
)

modified_product_stg2ods_task = PythonOperator(
    task_id='modified_product_stg2ods_task',
    provide_context=True,
    python_callable=load_stg2ods,
    on_failure_callback=dag_failure_handler,
    dag=dag,
)

# modified_product_ods2edw_task = PythonOperator(
#    task_id='modified_product_ods2edw_task',
#    provide_context=True,
#    python_callable=load_ods2edw,
#    on_failure_callback=dag_failure_handler,
#    dag=dag,
# )

# create edw data task:
edw_lgc_product_modify_create = PythonOperator(
    task_id='r_edw_product_modify_delete',
    provide_context=True,
    python_callable=update_downstream,
    op_kwargs={'myutil': myutil, 'gpdb': db, 'sql_file_name': "lgc_modified_product",
               'sql_section': 'create_table_query', 'args': args},
    on_failure_callback=dag_failure_handler,
    dag=dag,
)

# delete from edw data task:
# insert into edw data task:
edw_lgc_product_modify_insert = PythonOperator(
    task_id='edw_lgc_product_modify_insert',
    provide_context=True,
    python_callable=update_downstream,
    op_kwargs={'myutil': myutil, 'gpdb': db, 'sql_file_name': "lgc_modified_product",
               'sql_section': 'insert_table_query', 'args': args},
    on_failure_callback=dag_failure_handler,
    dag=dag,
)

postprocess_modified_product_task = PythonOperator(
    task_id='postprocess_modified_product_task',
    provide_context=True,
    python_callable=post_process_fileload,
    op_kwargs={'is_encrypted': False},
    on_failure_callback=dag_failure_handler,
    dag=dag,
)

preprocess_modified_product_task >> modified_product_src2stg_task >> modified_product_stg2ods_task >> edw_lgc_product_modify_create
edw_lgc_product_modify_create >> edw_lgc_product_modify_insert >> postprocess_modified_product_task
