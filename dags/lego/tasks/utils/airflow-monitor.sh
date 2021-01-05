#!/bin/sh
MAIL_TO="zhxie@deloitte.com.cn,jamzeng@deloitte.com.cn,spwei@deloitte.com.cn"
MAIL_SUBJECT='PROD AIRFLOW SERVER DOWN'
AIRFLOW_WORK_DIR="/cdp/work_dir"
log(){
    echo $(date):$1 >> /cdp/work_dir/airflow_monitor.log
}

start_airflow_webserver(){
    log "starting airflow webserver"
    systemctl start airflow-webserver
}

start_airflow_scheduler(){
    log "starting airflow scheduler"
    systemctl start airflow-scheduler
}

start_airflow_dag(){
    dag_name=$1
	log " Try to start dag $dag_name"
    airflow trigger_dag -sd "/root/airflow/dags/" $dag_name
}

sendout_mail(){
    msg=$1
    log "sendout mail:$msg"
    python3 ${AIRFLOW_WORK_DIR}/lego_send_mail.py -t "$MAIL_TO" -s "$MAIL_SUBJECT" -c "$msg"
}

log "Starting check airflow server state..."
export AIRFLOW_HOME=/cdp/airflow
AIRFLOW_SCHEDULER=$(ps -ef|grep "airflow scheduler"|grep -v grep |wc -l)
AIRFLOW_WEBSERVER=$(ps -ef|grep "airflow-webserver"|grep -v grep |wc -l)

# airflow list_dag_runs monitoring_file_dag --state running | tail -n -1 | grep "\d*\s*\|.*"
AIRFLOE_MONITOR_RUNNING=$(airflow list_dag_runs monitoring_file_dag --state running | tail -n -1 | grep -E "\d*\s*\|.*"|wc -l)
#0b0000 airflow scheduler alive, airflow webserver alive 
#0b0001 airflow scheduler stopped, airflow webserver alive 
#0b0010 airflow scheduler alive, airflow webserver stopped
#0b0011 airflow scheduler stopped, airflow webserver stopped
STATE_FLAG=2#0000

AIRFLOW_SCHEDULERF_FLAG=2#0000
AIRFLOW_WEBSERVER_FLAG=2#0000

if [[ $AIRFLOW_SCHEDULER  -lt 2 ]] ; then
    AIRFLOW_SCHEDULERF_FLAG=2#0001
fi    

if [[ $AIRFLOW_WEBSERVER  -lt 2 ]] ; then
    AIRFLOW_WEBSERVER_FLAG=2#0010
fi 

let "STATE_FLAG=$AIRFLOW_SCHEDULERF_FLAG|$AIRFLOW_WEBSERVER_FLAG"

if [[ $STATE_FLAG  -eq 1 ]]; then
    #start airflow scheduler
    start_airflow_scheduler
    #sendout warning mail
    if [[ $? -eq 0 ]];then
        sendout_mail "Airflow scheduler has been restarted"
    else
        sendout_mail "Airflow scheduler is down and cannot be restarted"
    fi
elif [[ $STATE_FLAG  -eq 2 ]]; then
    #start airflow webserver
    start_airflow_webserver
    #sendout warning mail
    
    if [[ $? -eq 0 ]];then
        sendout_mail "Airflow webserver has been restarted"
    else
        sendout_mail "Airflow webserver is down and cannot be restarted"
    fi
elif  [[ $STATE_FLAG  -eq 3 ]]; then
    #start airflow webserver and sentout email
    start_airflow_webserver && start_airflow_scheduler
    #sendout warning mail
    if [[ $? -eq 0 ]];then
        sendout_mail "Airflow webserver and scheduler have been restarted"
    else
        sendout_mail "Airflow webserver and scheduler are down and cannot be restarted"
    fi
else
    log "Airflow works well."
fi


if [[ $STATE_FLAG -eq 0 ]] && [[ $AIRFLOE_MONITOR_RUNNING -eq 0 ]]; then
    # monitor stopped, need to restart 
    log "monitoring_file_dag stopped. Need to restart it."
    start_airflow_dag "monitoring_file_dag"

     if [[ $? -eq 0 ]];then
        log "Airflow [monitoring_file_dag] have been restarted"
        sendout_mail "Airflow [monitoring_file_dag] have been restarted"
    else
        log "Airflow [monitoring_file_dag] is STOPPED and cannot be restarted"
        sendout_mail "Airflow [monitoring_file_dag] is STOPPED and cannot be restarted"
    fi
fi

exit 0
