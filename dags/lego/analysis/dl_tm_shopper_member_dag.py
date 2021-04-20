from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow import DAG

from datetime import datetime
import sys

DAG_HOME =  Variable.get('dag_home').strip().rstrip('/')
sys.path.append("../tasks/")
sys.path.append(DAG_HOME + "/tasks/")
from utils.myutil import Myutil

# variable to run the shell scripts
TEMP_FOLDER='Temp'
SRC_NAME = "DL"
entity = 'tm_shopper'
src_entity = 'dl_tm_shopper'
DAG_NAME = 'dl_tm_shopper_dag'
dag_start_date = Variable.get('dag_start_date').strip()
tm_shopper_member_interval = Variable.get('interval_tm_shopper_member').strip()

myutil = Myutil(dag_home=DAG_HOME, entity_name=src_entity)
db = myutil.get_db()
entity_conf = myutil.get_entity_config()
email_to_list =  Variable.get('email_to_list').split(',')

shopper_member_query_sections = ['daily_update_shopper']
tm_order_theme_query_sections = ['tm_order_theme_update']
shopper_batchdate = datetime.strftime(datetime.now(),'%Y%m%d')

args = {
    'owner': 'cdp_admin',
    'email': email_to_list,
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'max_active_runs': 1,
    "src_name": SRC_NAME
}

dag = DAG(dag_id = DAG_NAME,
            default_args = args,
            concurrency = 1, 
            max_active_runs = 1, 
            schedule_interval = tm_shopper_member_interval,
            catchup=False,
            start_date = datetime.strptime(dag_start_date,'%Y-%m-%d %H:%M:%S'),
            )
def dag_failure_handler(context):
    #rename: change prefix to "failed-"
    pass

def run_composit_task(query_sections:list, **kwargs):
    sql_dict = myutil.get_sql_yml_fd(src_entity)
    with db.create_session() as session:
        for set in query_sections:
            query = (sql_dict['EDW'][set]).format_map({"dl_batch_date":shopper_batchdate})
            session.execute(query)

shopper_member_update_task = PythonOperator(
    task_id='shopper_member_update_task',
    provide_context = True,
    python_callable = run_composit_task,
    op_kwargs = {'query_sections': shopper_member_query_sections},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)


tm_order_theme_update_task = PythonOperator(
    task_id='tm_order_theme_update_task',
    provide_context = True,
    python_callable = run_composit_task,
    op_kwargs = {'query_sections': tm_order_theme_query_sections},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)


shopper_member_update_task >> tm_order_theme_update_task