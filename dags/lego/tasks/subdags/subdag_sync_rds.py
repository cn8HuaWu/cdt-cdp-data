# -*- coding: utf-8 -*-
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import logging
import sys

DAG_HOME =  Variable.get('dag_home').strip().rstrip('/')
sys.path.append(DAG_HOME + "/tasks/")
sys.path.append("../../tasks/")
from utils.db import Mydb

def sync_subdag(parent_dag_name, child_dag_name, myutil, entity_conf, args, ientity):
    rds_host = myutil.get_conf( 'RDS', 'RDS_HOST')
    rds_port = myutil.get_conf( 'RDS', 'RDS_PORT')
    rds_db = myutil.get_conf( 'RDS', 'RDS_DB')
    rds_usr = myutil.get_conf( 'RDS', 'RDS_USER')
    rds_pw = myutil.get_conf( 'RDS', 'RDS_PASSWORD')
    rdsdb = Mydb(rds_host, rds_port, rds_db, rds_usr, rds_pw)

    def run_update(**kwargs):
        engine = rdsdb.create_engine()
        conn =  rdsdb.create_conn( engine )
        entity = ientity 
        sync_list = []
        src_entity = args["src_name"].lower()+"_"+entity
        if ('dm_sync' not in  entity_conf[src_entity] or  entity_conf[src_entity]['dm_sync'] ):
            sync_list.append(src_entity)
            logging.info("Need to sync the original table: %s", entity)

        if('dm_dependicies' in entity_conf[src_entity] ):
            sync_list.extend( entity_conf[src_entity]['dm_dependicies'].split(',') )

        for ent in sync_list:
            logging.info(" sync table %s to RDS", ent)
            sql_dict = myutil.get_sql_yml_fd(ent.strip())
            query = sql_dict['EDW']['sync_to_rds_dm']

            logging.info("Run update query %s", query)
            rdsdb.execute(query,conn)
        rdsdb.close_conn(conn)
    
    dag_subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args = args,
        schedule_interval = None,
    )

    PythonOperator(
        task_id='%s-task' % (child_dag_name),
        default_args=args,
        dag=dag_subdag,
        provide_context = True,
        python_callable = run_update,
    )

    return dag_subdag


