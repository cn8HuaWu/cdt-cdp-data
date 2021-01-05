import logging
from datetime import datetime
from airflow.operators.sensors import BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)

class DatahubSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, mydb, dh_id, myutil, max_poke=15, *args, **kwargs):
        self.mydb = mydb
        self.dh_id = dh_id
        self.myutil = myutil
        self.sql_dict = self.myutil.get_sql_yml_fd('datahub_oss_stg_check')
        self.engine = self.mydb.create_engine()
        self.conn = self.mydb.create_conn(self.engine)
        self.latest_count = self._get_datahub_saved_count()
        self.update_flag = False
        self.max_pork = max_poke
        super(DatahubSensor, self).__init__(*args, **kwargs)
 
    def poke(self, context):
        new_count = self._get_datahub_latest_count()
        log.info("new_count: " + str(new_count))
        if (self.max_pork <= 0 or self.latest_count == new_count):
            try:
                log.info("Push the datahub updated flag into xcom:" + str(self.update_flag))
                self._put_datahub_update_flag(context, self.update_flag)
                if self.update_flag:
                    self._save_datahub_latest_count()
                return True
            except ValueError:
                log.error("Failed to push update flag to xcom: " + self.dh_id)
                raise ValueError("Failed to push update flag to xcom"  + self.dh_id)
            except:
                log.error("Failed to save the new data count "  + self.dh_id)
                raise ValueError("Failed to save the new data count "  + self.dh_id)
            finally:
                self.mydb.close_conn(self.conn)
        else:
            self.update_flag = True
            self.max_pork -= 1
            self.latest_count = new_count
            log.info("New data pushed into Datahub")
            return False

    def _put_datahub_update_flag(self, context, update_flag):
        context['task_instance'].xcom_push(key='new_data_flag', value=str(update_flag))

    def _get_datahub_latest_count(self):
        new_count_sql = self.sql_dict[self.dh_id+"_new"]
        rst = self.mydb.execute(new_count_sql, self.conn)
        count = rst.first()
        return int(count[0])
    
    def _get_datahub_saved_count(self):
        saved_count_sql = self.sql_dict[self.dh_id+"_saved"]
        rst = self.mydb.execute(saved_count_sql, self.conn)
        count = rst.first()
        return 0 if count is None else int(count[0])

    def _save_datahub_latest_count(self):
        update_count_sql = self.sql_dict[self.dh_id+"_update"]
        log.info("Latest record count is " + str(self.latest_count))
        update_count_sql = update_count_sql.replace("?", str(self.latest_count))
        self.mydb.execute(update_count_sql, self.conn)

class CDPAriflowPlugin(AirflowPlugin):
    name = "cdp-airflow-plugin"
    operators = [DatahubSensor]