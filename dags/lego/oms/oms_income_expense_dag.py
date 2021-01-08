from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.executors.sequential_executor import SequentialExecutor
from airflow.models import Variable

from airflow import DAG
from airflow.operators import DatahubSensor
from airflow.utils.db import provide_session
from airflow.models import XCom
import  sys
from datetime import datetime,timedelta
from pytz import timezone

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
SRC_NAME = "OMS"
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
entity = 'income_expense'
src_entity = 'oms_income_expense'
DAG_NAME = 'oms_income_expense_dag'

has_head = 0
data_timedelta = int(Variable.get('oms_data_timedelta').strip())
batch_date = datetime.strftime( datetime.now(timezone('Asia/Shanghai')) - timedelta(days = data_timedelta ) , '%Y%m%d')
api_interval = Variable.get('interval_oms_report').strip()
dag_start_date = Variable.get('dag_start_date').strip()

hr_work_time = 11
lego_hr_mail_to = Variable.get('lego_hr_mail_to').strip().split(",") 
lego_hr_subject = Variable.get('lego_hr_subject').strip() + "-INCOME EXPRENSE" 
lego_hr_html_content =  "INCOME EXPRENSE " + Variable.get('lego_hr_html_content').strip() 
lego_hr_html_content_premail =  "INCOME EXPRENSE Starts running" 

@provide_session
def cleanup_xcom(context, session=None):
    session.query(XCom).filter(XCom.dag_id == DAG_NAME).delete()

def post_process_fileload( **kwargs):
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


def load_stg2ods(**kwargs):
    pkey = entity_conf[src_entity]["key"]
    stg_suffix = entity_conf[src_entity]["stg_suffix"]
    kwargs['task_instance'].xcom_push(key='batch_date', value=batch_date)
    stg2ods = Stg2odsHandler(TEMP_FOLDER, STAGING, ODS, batch_date, SRC_NAME, entity, stg_suffix, pkey, myutil, db, has_head = has_head )
    stg2ods.start()

def load_ods2edw(**kwargs):
    pkey = entity_conf[src_entity]["key"]
    table_prefix = entity_conf[src_entity]["edw_prefix"]
    update_type = entity_conf[src_entity]["update_type"]
    #
    my_batch_date = kwargs['task_instance'].xcom_pull(key='batch_date', task_ids='oms_order_stg2ods_task')
    ods2edw = Ods2edwHandler( my_batch_date, 
        SRC_NAME, 
        entity, 
        pkey,
        table_prefix, 
        myutil, 
        db, 
        has_param=True  )
    ods2edw.start()

def b_choice(**kwargs):
    updated_flag = kwargs['task_instance'].xcom_pull(key='new_data_flag', task_ids='income_expense_sensor_task')
    if ( updated_flag in('', 'False') ):
        return "postprocess_income_expense_task"
    else:
        return "premail_or_not_branch_task"

def mail_or_not(**kwargs):
    hour = datetime.now().hour
    if hour >= hr_work_time:
        return "income_expense_email_task"
    else:
        return "postprocess_income_expense_task"

def premail_or_not(**kwargs):
    hour = datetime.now().hour
    if hour >= hr_work_time:
        return "income_expense_premail_task"
    else:
        return "oms_income_expense_stg2ods_task"

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
    "batch_date_args": 'oms_income_expense_stg2ods_task',
}

dag = DAG(dag_id = DAG_NAME,
            default_args = args,
            concurrency = 1, 
            max_active_runs = 1, 
            catchup=False,
            on_success_callback=cleanup_xcom,
            schedule_interval = api_interval)

income_expense_sensor_task = DatahubSensor(
    mydb=db, 
    dh_id=src_entity, 
    myutil= myutil, 
    task_id='income_expense_sensor_task', 
    poke_interval=120, 
    dag=dag
)

premail_or_not_branch_task = BranchPythonOperator(
    task_id="premail_or_not_branch_task",
    python_callable= premail_or_not,
    provide_context = True,
    dag=dag,
)

income_expense_premail_task = EmailOperator(
    task_id="income_expense_premail_task",
    dag=dag,
    on_failure_callback = dag_failure_handler,
    to = lego_hr_mail_to,
    subject = lego_hr_subject,
    html_content = lego_hr_html_content_premail
)

oms_income_expense_stg2ods_task = PythonOperator(
    task_id='oms_income_expense_stg2ods_task',
    provide_context = True,
    python_callable = load_stg2ods,
    on_failure_callback = dag_failure_handler,
    trigger_rule='one_success',
    dag=dag,
)

oms_income_expense_ods2edw_task = PythonOperator(
    task_id='oms_income_expense_ods2edw_task',
    provide_context = True,
    python_callable = load_ods2edw,
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

check_datahub_branch_task = BranchPythonOperator(
    task_id='check_datahub_branch_task',
    python_callable= b_choice,
    provide_context = True,
    dag=dag,
)

oms_order_update_promotion_info_task = PythonOperator(
    task_id='oms_order_update_promotion_info_task',
    provide_context = True,
    python_callable = update_downstream,
    op_kwargs = {'myutil':myutil, 'gpdb': db, 'sql_file_name':"oms_promotion_info" , 'sql_section': 'update_by_income_expense', 'args': args},
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

mail_or_not_branch_task = BranchPythonOperator(
    task_id="mail_or_not_branch_task",
    python_callable= mail_or_not,
    provide_context = True,
    dag=dag,
)

income_expense_email_task = EmailOperator(
    task_id="income_expense_email_task",
    dag=dag,
    on_failure_callback = dag_failure_handler,
    to = lego_hr_mail_to,
    subject = lego_hr_subject,
    html_content = lego_hr_html_content
)

income_expense_sync_2_rds_task = SubDagOperator(
    task_id='income_expense_sync_2_rds_task',
    subdag=sync_subdag(DAG_NAME, 'income_expense_sync_2_rds_task', myutil, entity_conf, args, entity),
    default_args=args,
    executor=SequentialExecutor(),
    on_failure_callback = dag_failure_handler,
    dag=dag,
)

postprocess_income_expense_task = PythonOperator(
    task_id = 'postprocess_income_expense_task',
    provide_context = True,
    python_callable = post_process_fileload,
    op_kwargs = {'is_encrypted': False},
    on_failure_callback = dag_failure_handler,
    trigger_rule='none_failed',
    dag = dag,
)

income_expense_sensor_task >> check_datahub_branch_task >> premail_or_not_branch_task >> income_expense_premail_task >> oms_income_expense_stg2ods_task >> oms_income_expense_ods2edw_task >> oms_order_update_promotion_info_task >>income_expense_sync_2_rds_task >>  mail_or_not_branch_task >> income_expense_email_task >> postprocess_income_expense_task
premail_or_not_branch_task >> oms_income_expense_stg2ods_task
mail_or_not_branch_task >> postprocess_income_expense_task
check_datahub_branch_task >> postprocess_income_expense_task