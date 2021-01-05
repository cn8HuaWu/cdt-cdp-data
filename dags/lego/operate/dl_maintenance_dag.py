from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow import DAG

import logging
import os, sys
from datetime import datetime

DAG_HOME =  Variable.get('dag_home').strip().rstrip('/')
sys.path.append("../tasks/")
sys.path.append(DAG_HOME + "/tasks/")

from utils.clear_server_file import start_clean
from utils.db import Mydb
from datetime import timedelta
from utils.myutil import Myutil

DAG_NAME = 'dl_maintenance_dag'
product_interval = Variable.get('interval_maintenance').strip()
dag_start_date = Variable.get('dag_start_date').strip()

myutil = Myutil(DAG_HOME)
gp_host = myutil.get_conf( 'Greenplum', 'GP_HOST')
gp_port = myutil.get_conf( 'Greenplum', 'GP_PORT')
gp_db = myutil.get_conf( 'Greenplum', 'GP_DB')
gp_usr = myutil.get_conf( 'Greenplum', 'GP_USER')
gp_pw = myutil.get_conf( 'Greenplum', 'GP_PASSWORD')
db = Mydb(gp_host, gp_port, gp_db, gp_usr, gp_pw)

email_to_list =  Variable.get('email_to_list').split(',')
vacuum_shell_path = os.path.join(DAG_HOME,"tasks/sqls/database_maintenance.sh")

args = {
    'owner': 'cdp_admin',
    'email': email_to_list,
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'max_active_runs': 1,
}

dag = DAG(dag_id = DAG_NAME,
            default_args = args,
            concurrency = 1, 
            max_active_runs = 1, 
            start_date = datetime.strptime(dag_start_date,'%Y-%m-%d %H:%M:%S'),
            schedule_interval = product_interval,
        )

vacuum_run_command = '''
    bash %s "%s" "%s" "%s" "%s" "%s"
'''%(vacuum_shell_path, gp_host, gp_port, gp_usr, gp_pw, gp_db)

vacuum_tables_task = BashOperator(
    task_id='vacuum_tables_task',
    bash_command= vacuum_run_command,
    dag=dag,
)


def clear_old_file( **kwargs):
    old_day = 60
    MONITOR_PATH = Variable.get('monitor_path')  
    start_clean(MONITOR_PATH, old_day)


clear_server_file_task = PythonOperator(
    task_id='clear_server_file_task',
    provide_context = True,
    python_callable = clear_old_file,
    dag=dag,
)


