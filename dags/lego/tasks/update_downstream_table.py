# -*- coding: utf-8 -*-
from airflow.models import DAG
from airflow.models import Variable
import logging
import sys

def update_downstream( myutil, gpdb, sql_file_name, sql_section, args, staging='EDW' , **kwargs):
    debug_flg = myutil.get_conf('ETL','SQL_DEBUG')
    if (debug_flg.upper() == 'TRUE'):
        dgb_ind = True
    else:
        dgb_ind = False

    engine = gpdb.create_engine(debug_flag = dgb_ind)
    conn =  gpdb.create_conn( engine )
    sql_dict = myutil.get_sql_yml_fd(sql_file_name)
    if (kwargs.get('dag_run').conf is not None and 'batch_date' in kwargs.get('dag_run').conf):
        batch_date = kwargs.get('dag_run').conf.get('batch_date')   
    else:
        batch_date = kwargs['task_instance'].xcom_pull(key='batch_date', task_ids=args["batch_date_args"])
    myparams = dict(batch_date=batch_date)
    query = sql_dict[staging][sql_section].format_map(myparams)
    
    gpdb.execute(query,conn)
    gpdb.close_conn(conn)
