from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow import DAG
import  sys
from datetime import datetime

DAG_HOME =  Variable.get('dag_home').strip().rstrip('/')
sys.path.append(DAG_HOME + "/tasks/")
sys.path.append("../tasks/")
from utils.myutil import Myutil
from utils.db import Mydb
from update_downstream_table import update_downstream
from ods2edw import Ods2edwHandler

# variable to run the shell scripts
SRC_NAME = "DL"

myutil = Myutil(DAG_HOME)
dl_aes_key = myutil.get_dl_aes_key()
dl_aes_iv = myutil.get_dl_aes_iv()

gp_host = myutil.get_conf( 'Greenplum', 'GP_HOST')
gp_port = myutil.get_conf( 'Greenplum', 'GP_PORT')
gp_db = myutil.get_conf( 'Greenplum', 'GP_DB')
gp_usr = myutil.get_conf( 'Greenplum', 'GP_USER')
gp_pw = myutil.get_conf( 'Greenplum', 'GP_PASSWORD')
db = Mydb(gp_host, gp_port, gp_db, gp_usr, gp_pw)

entity_conf = myutil.get_entity_config()

email_to_list =  Variable.get('email_to_list').split(',')
DAG_NAME = 'dl_mini_member_distribution_dag'
entity = 'mini_member_distribution'
src_entity = 'dl_mini_member_distribution'

batch_date = datetime.strftime(datetime.now(),'%Y%m%d')
dag_start_date = Variable.get('dag_start_date').strip()
mini_member_interval = Variable.get('mini_member_interval').strip()

def post_process_fileload( **kwargs):
    #rename: change prefix to "done-"
   pass

def dag_failure_handler(context):
    #rename: change prefix to "failed-"
    pass

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
        update_type = update_type,
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
    'start_date': datetime.strptime(dag_start_date,'%Y-%m-%d %H:%M:%S'),
    'max_active_runs': 1,
    "src_name": SRC_NAME,
    "batch_date_args": 'preprocess_refresh_mly_task'
}

dag = DAG(dag_id = DAG_NAME,
            default_args = args,
            concurrency = 1, 
            max_active_runs = 1, 
            catchup=False,
            schedule_interval = mini_member_interval)


refresh_mini_member_distribution_task = PythonOperator(
    task_id='refresh_mini_member_distribution_task',
    provide_context = True,
    python_callable = load_ods2edw,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

stats_mini_member_by_gis_task = PythonOperator(
    task_id='stats_mini_member_by_gis_task',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"dl_mini_member_distribution" , 'sql_section': 'update_mini_member_month_stats', 'args': args},
    dag=dag,
)


refresh_mini_member_distribution_task   >> stats_mini_member_by_gis_task