# -*- coding: utf-8 -*-
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import logging
import sys

def update_subdag(parent_dag_name, child_dag_name, myutil, gpdb, args, ientity=None):
 
    def run_update(**kwargs):
        engine = gpdb.create_engine()
        conn =  gpdb.create_conn( engine )
        entity = ientity if (ientity is not None) else kwargs.get('dag_run').conf.get('entity')
        batch_date = kwargs.get('dag_run').conf.get('batch_date')
        sql_dict = myutil.get_sql_yml_fd(args["src_name"].lower()+"_"+entity)
        query = sql_dict['EDW']['post_update_other_edw'].format(batch_date)

        logging.info("Run update query %s", query)
        gpdb.execute(query,conn)
        gpdb.close_conn(conn)
        
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


