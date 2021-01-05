import os, sys
from datetime import datetime
import time
import logging

class file_cleaner:
    def __init__(self, path, days_ago):
        super().__init__()
        self.path = path
        self.days_ago = days_ago
    
    # >60
    def stat_file(self ):
        need_clear_list = []
        dtime = datetime.now()
        ans_time = time.mktime(dtime.timetuple())
        
        for root, dir, files in os.walk(self.path):
            for file in files:
                full_path = os.path.join(root, file)
                mtime = os.stat(full_path).st_mtime
                if( ans_time - mtime > self.days_ago*24*60*60 and not self.excep_rule(full_path)):
                    need_clear_list.append(full_path)
        return need_clear_list

    def excep_rule(self, full_path):
        return False

    ## TO DO. NEED the rules
    def clean_file(self, file_list):
        for file in file_list:
            logging.info(file_list)

    def start(self):
        file_list =   self.stat_file()   
        self.clean_file(file_list)     

def start_clean(path, dags_ago):
    cleaner = file_cleaner(path, dags_ago)
    cleaner.start()

if __name__ == "__main__":
    start_clean("C:\\\\workspace\\project\\LEGO\\code\\airflow-jobs\\dags\\tasks\\utils", 5)
    