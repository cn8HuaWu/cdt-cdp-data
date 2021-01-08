from airflow.models import Variable
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import imp

email_to_list =  Variable.get('email_to_list')
dag_start_date = Variable.get('dag_start_date').strip()
api_interval = Variable.get('interval_data_scan_report').strip()
DAG_HOME =  Variable.get('dag_home').strip().rstrip('/')

DAG_NAME ='dl_data_quality_scan'

def quality_scan_n_send_mail(**kwargs):
    cdp_rete = imp.load_source('cdp_rete', DAG_HOME+"/tasks/utils/cdp_rete.py" )
    (err, warning) = cdp_rete.excute_rete_from_db()
    
    if not err or not warning:
        email_body = cdp_rete.generate_data_scan_report(err, warning, ) 
        cdp_email = imp.load_source('lego_send_mail', DAG_HOME+"/tasks/utils/lego_send_mail.py" )
        cdp_email.send_email( email_to_list, "cdp daily report", email_body)

args = {
    'owner': 'cdp_admin',
    'email': email_to_list,
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': datetime.strptime(dag_start_date,'%Y-%m-%d %H:%M:%S'),
    'max_active_runs': 1,
    'retries': 0
}

dag = DAG(dag_id = DAG_NAME,
            default_args = args,
            concurrency = 1, 
            max_active_runs = 1, 
            catchup=False,
            schedule_interval = api_interval)

scan_data_quality_task = PythonOperator(
    task_id='scan_data_quality_task',
    provide_context = True,
    python_callable = quality_scan_n_send_mail,
    dag=dag,
)