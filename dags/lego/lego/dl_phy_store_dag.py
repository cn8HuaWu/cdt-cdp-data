from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow import DAG
from airflow.utils.db import provide_session
from airflow.models import XCom

import  sys
from datetime import datetime

DAG_HOME =  Variable.get('dag_home').strip().rstrip('/')
sys.path.append("../tasks/")
sys.path.append(DAG_HOME + "/tasks/")

from utils.myutil import Myutil

# variable to run the shell scripts
SRC_NAME = "DL"
STAGING = 'Staging'
ODS = 'ODS'
TEMP_FOLDER='Temp'

entity = 'phy_store_dag'
src_entity = 'dl_phy_store'
DAG_NAME = 'dl_phy_store_dag'
email_to_list =  Variable.get('email_to_list').split(',')

myutil = Myutil(dag_home=DAG_HOME, entity_name=src_entity)
db = myutil.get_db()

dag_start_date = Variable.get('dag_start_date').strip()
update_attributions = ['merge_lgc_lewin_stores']

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
            schedule_interval = None,
)

def run_composit_task(query_sections:list, **kwargs):
    sql_dict = myutil.get_sql_yml_fd(src_entity)
    # batch_date = kwargs.get('dag_run').conf.get('batch_date')
    with db.create_session() as session:
        for set in query_sections:
            query = (sql_dict['EDW'][set])
            session.execute(query)

@provide_session
def cleanup_xcom(context, session=None):
    session.query(XCom).filter(XCom.dag_id == DAG_NAME).delete()

def dag_failure_handler(context):
    cleanup_xcom(context)

dl_phy_store_update_task = PythonOperator(
    task_id='dl_phy_store_update_task',
    provide_context = True,
    python_callable = run_composit_task,
    op_kwargs = {'query_sections': update_attributions},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

dl_phy_store_update_task