import subprocess, os
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import datetime
import json
import re

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import DagNotFound, DagFileExists
from airflow import configuration as conf
from airflow.models import DagBag, DagRun
from airflow.models import Variable
from airflow.utils.state import State


MONITOR_PATH = Variable.get('monitor_path') # "it's the SFTP data folder"
MAX_RETRY =  int(Variable.get('max_retries'))

FILENAME_PATTERN = json.loads(Variable.get('filename_pattern'))
email_to_list =  Variable.get('email_to_list').split(',')

BATCH_DATE_IN_NAME =  ['mini_member_info']
class MonitorEventHandler(FileSystemEventHandler):
    """Logs all the events captured."""

    def on_moved(self, event):
        pass
     #   super(MonitorEventHandler, self).on_moved(event)

        # what = 'directory' if event.is_directory else 'file'
        # logging.info("Moved %s: from %s to %s", what, event.src_path,
        #              event.dest_path)

    def on_created(self, event):
        super(MonitorEventHandler, self).on_created(event)

        what = 'directory' if event.is_directory else 'file'
        logging.info("Created %s: %s", what, event.src_path)
        start_job_process(event.src_path)
        
    def on_deleted(self, event):
        pass
     #   super(MonitorEventHandler, self).on_deleted(event)

        # what = 'directory' if event.is_directory else 'file'
        # logging.info("Deleted %s: %s", what, event.src_path)

    def on_modified(self, event):
        pass
     #   super(MonitorEventHandler, self).on_modified(event)
        
        # what = 'directory' if event.is_directory else 'file'
        # logging.info("Modified %s: %s", what, event.src_path)
        # logging.info("event %s", str(event))


logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')



from multiprocessing import Process

def start_job_process(file_path):
    FILE_2_DAG_STR = Variable.get('filename_2_dag_map')
    FILE_2_DAG_JSON = json.loads(FILE_2_DAG_STR)

    job_process = Process( target=_start_job_process, args=(file_path,FILE_2_DAG_JSON) )
    job_process.daemon = True
    job_process.start()

def _start_job_process(file_path, FILE_2_DAG_JSON):
    # file name cannot have dot
    file_name = os.path.basename(file_path).split('.')[0]
   
    dag_key = get_dag_key(file_name)
    # ignore the file if:
    #   batch_date is not qualified 
    #   it's not OK file
    if( not is_valid_file(file_path, dag_key, FILE_2_DAG_JSON) ):
        return

    batch_date = get_batch_date(dag_key, FILE_2_DAG_JSON)
    if( batch_date is None):
        return 
    
    # the batch date MUST BE folder name previouly to the data file
    batch_date_in_path = get_input_batch_date(file_path, file_name, dag_key, FILE_2_DAG_JSON)
    if ( batch_date_in_path is None ):
        logging.error("The naming of path is wrong.dag_key: %s . batch date in path : %s. ok_file_path: %s", dag_key, batch_date_in_path, file_path)
        return 

    monitor_history = FILE_2_DAG_JSON[dag_key]["history"]
    if( monitor_history != 'Y' and batch_date != batch_date_in_path ):
        logging.error("batch in the ok file path should be ignore. batch in filepath: %s, Batch date should be : %s. ok_file_path: %s", batch_date_in_path, batch_date, file_path)
        return 

    batch_date = batch_date_in_path
    #Control the active dags <= 2
    try:
        dag_id = find_dag_by_filename(file_path, dag_key, FILE_2_DAG_JSON)
        job_list = get_running_jobs( dag_id)
    except Exception as err:
        logging.error("Cannot find the dag_id by the file_path: %s", file_path)
        logging.error(err)
        #TODO send out a mail
        return
    except:
        logging.error("Cannot find the dag_id by the file_path: %s", file_path)
        # TODOsend out a mail
        return

    # if =<1 job is running, trigger it. 
    # the later one get started once the preivous finished
    # if more than 1 jobs are running, should skip it
    tried = 0
    if( len(job_list) < 2  ):
        logging.info("%s jobs are running. Trigger the dag: %s", str(len(job_list)),  dag_id)
        tried = 0
        while( tried <= MAX_RETRY):
            try:
                if ( len(batch_date)<8 ):
                    batch_date = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d')
                trg = trigger_airflow_job(dag_id, file_path, dag_key, batch_date)
                if( trg is None ):
                    tried += 1
                    continue
                break
            except DagNotFound as err:
                logging.warning( "The dag id:%s does not exist"%(dag_id) )
                logging.error(str(err))
                break
            except Exception as err:
                logging.warning( "Failed to start the dag" )
                logging.warning( str(err) )
                tried += 1
            except:
                logging.warning( "Failed to start the dag" )
                tried += 1
    else:
        logging.info(" >2 jobs are running. The pending dag will load the new coming file" )

    if( tried == MAX_RETRY ):
        # send out the warning email to the amin
        # cannot start the job
        logging.error("Failed to start the job: %s" , dag_id)

import re
def get_input_batch_date( file_path, file_name, dag_key, FILE_2_DAG_JSON):
    logging.info("file_path: %s, file_name： %s, dag_key: %s", file_path, file_name, dag_key )
    entity = FILE_2_DAG_JSON[dag_key]['entity']
    if ( entity in BATCH_DATE_IN_NAME ):
        file_batch = file_name.split('_')[-1].split('.')[0].replace("-","")

        pattern = re.compile(r'\d{8}')
        if ( pattern.findall(file_batch) ):
            return file_batch
        else:
            return None
    else:
        return file_path.split('/')[-2]

def get_batch_date(dag_key, FILE_2_DAG_JSON):
    # if delta_days = 'M', then batch_date = Month( today), return YYYYMM
    # if delta_days isDigit, then batch_date = today - delta_days, return YYYYMMDD
    delta_days = FILE_2_DAG_JSON[dag_key]["delta_days"]
    if( delta_days.upper() == 'M' ):
        # return datetime.datetime.now().strftime('%Y%m')
        now_time = datetime.datetime.today()
        now_month = now_time.month
        now_year = now_time.year
        return datetime.datetime.strftime(datetime.datetime(now_year, now_month, 1) - datetime.timedelta(days=1),'%Y%m')
    elif( str(delta_days).isdigit ):
        return datetime.datetime.strftime(datetime.datetime.today() - datetime.timedelta(days=int(delta_days)), '%Y%m%d')
    else:
        logging.error("The 'delta_days' config is not right, please check  filename_2_dag_map Variable ")
        return None


def get_ds_by_file_path(file_path):
    source_name = file_path[len(MONITOR_PATH):].lstrip('/').split('/')[0].upper()
    return source_name

def get_dag_key(file_name):
    file_name = file_name.upper()
    for key in FILENAME_PATTERN.keys():
        pattern = re.compile(key)
        if( len(pattern.findall(file_name.upper())) > 0):
            return FILENAME_PATTERN[key]
    
    return file_name

def is_valid_file(file_path, dag_key, FILE_2_DAG_JSON):
    # ok file:  means put a datafile to the folder
    if( os.path.basename(file_path).split('.')[-1].upper() != 'OK'):
        logging.error("Not ok file, ignore it! File path: %s", file_path)
        return False

    if(dag_key not in FILE_2_DAG_JSON):
        logging.error("Can not find a dag by file_name: %s, dag_key is: %s ,  please contact amdin to check the filename_2_dag_map Variable ", file_path, dag_key)
        return False
    return True

def find_dag_by_filename(file_path, dag_key, FILE_2_DAG_JSON):
    #TODO need the file name pattern
    if( dag_key in FILE_2_DAG_JSON ):
        return FILE_2_DAG_JSON[dag_key]['dag']
    return None 

def get_running_jobs( dag_id ):
    dag_folder = conf.get('core','DAGS_FOLDER')
    dagbag = DagBag(dag_folder)
    running_list = DagRun.find(dag_id= dag_id, state = State.RUNNING)
    return running_list

from airflow.api.common.experimental import trigger_dag
def trigger_airflow_job(job_name, ok_file_path, dag_key, batch_date):
    man_run_id = 'manual__' + datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S%f')
    # CLI trigger 
    # subprocess.call(['airflow', 'trigger_dag','-sd', '/root/airflow/dags/', '-r',run_id , "-c",  run_conf, job_name ])

    # experimental api
    datasource = get_ds_by_file_path(ok_file_path)
    #TODO need confirm
    
    # entity = FILE_2_DAG_JSON[dag_key]['entity']
    file_name = os.path.basename(ok_file_path)[:-3]

    run_conf = {'ok_file_path':ok_file_path, 
                # 'entity':entity,
                'src_filename':file_name,
                'batch_date':batch_date
                }
    logging.info( "Trigger the dag: %s with config: %s"%( job_name, str(run_conf)) )
    trg = trigger_dag.trigger_dag(job_name, run_id=man_run_id, conf=run_conf )
    # trg = subprocess.call(['airflow', 'trigger_dag','-sd', '/root/airflow/dags/', '-r', man_run_id , '-c',  str(run_conf), job_name ])
    return trg

def start_monitor(path, **kwargs):
    event_handler = MonitorEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive= True)
    observer.start()
    try:
        while observer.isAlive():
            observer.join(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

args = {
    'owner': 'cdp_admin',
    'email': email_to_list,
    'email_on_failure': True,
    'email_on_retry': True,
    'depends_on_past': False,
    'start_date': days_ago(0),
    'max_active_runs':10
}

dag = DAG(dag_id='monitoring_file_dag',
        default_args=args,  
        schedule_interval='@once')


monitor_task = PythonOperator(
    task_id='monitor_file_task',
    provide_context=True,
    #pool='monitor_file_pool',
    python_callable=start_monitor,
    op_kwargs={'path': MONITOR_PATH},
    dag=dag,
)