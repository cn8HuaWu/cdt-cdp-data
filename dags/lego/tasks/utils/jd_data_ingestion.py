import requests 
import hashlib
import csv
import io
import sys, os, shutil
import gzip
import json
import logging
from datetime import datetime,timedelta
from pytz import timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class JDextractor(object):
    # :param days_to_load: defautly to load the data before today, if =0, extract all the historica data
    def __init__(self, table_name, url, signKey, save_path, column_list, days_to_load = 0, data_day_delta = 1, pageSize = 100, batch_date=None):
        super().__init__()
        self.table_name = table_name
        self.url = url
        self.signKey = signKey
        self.save_path = save_path
        self.column_list = column_list
        self.days_to_load = days_to_load  # source system time: T- days_to_load, 
        
        # raw data time: T - data_day_delta. For example, extract yesterday's data, but the data is about the day before yesterday
        self.data_day_delta = data_day_delta 
        self.pageSize = 100
        self.batch_date_yyyymmdd = batch_date or (datetime.now(timezone('Asia/Shanghai')) - timedelta(days = self.days_to_load )).strftime("%Y%m%d")
        self.file_batch_date_yyyymmdd = batch_date or (datetime.now(timezone('Asia/Shanghai')) - timedelta(days = self.data_day_delta )).strftime("%Y%m%d")

        logging.info("start to extract date on: " + self.batch_date_yyyymmdd)
        self.request_headers = {"Content-Type":"application/json"}

    def get_signed_params(self, pageNum, batch_date):
        
        # defining a params dict for the parameters to be sent to the API 
        PARAMS = {
            'pageNum': str(pageNum),
            'pageSize': str(self.pageSize),
            'timestamp': str(datetime.now(timezone('Asia/Shanghai')).strftime("%Y%m%d%H%M%S"))
        } 
        
        # get all historical data if days_to_load is 0, otherwise put in date condition 
        if (self.days_to_load == -1 ):
            startTime = '2020-05-21'
            batch_date = datetime.now(timezone('Asia/Shanghai')) - timedelta(days = 1 )
        else:
            startTime = batch_date.strftime("%Y-%m-%d")
        # # if self.days_to_load != 0:
        # logging.info('extract date, startTime:' + startTime + ' 00:00:00')
        # logging.info('endTime:' + (batch_date).strftime("%Y-%m-%d") + ' 23:59:59')

        PARAMS.update({'startTime': startTime + ' 00:00:00'})
        PARAMS.update({'endTime':(batch_date).strftime("%Y-%m-%d") + ' 23:59:59'})
        
        #join the parameters to be signed
        signTempStr = '&'.join([i + "=" + PARAMS[i] for i in sorted(PARAMS)]) + self.signKey
        
        #get the sign
        signValue = hashlib.md5(signTempStr.encode('utf-8')).hexdigest().upper()

        #add the sign to params dict
        PARAMS.update({'sign':signValue})
        
        return json.dumps(PARAMS)

    def requests_retry_session(self, retries=5, backoff_factor=3,status_forcelist=(500, 502, 503, 504), session=None):
        
        class CallbackRetry(Retry):
            def __init__(self, *args, **kwargs):
                self._callback = kwargs.pop('callback', None)
                super(CallbackRetry, self).__init__(*args, **kwargs)
            def new(self, **kw):
                # pass along the subclass additional information when creating a new instance.
                kw['callback'] = self._callback
                return super(CallbackRetry, self).new(**kw)
            def increment(self, method, url, *args, **kwargs):
                if self._callback:
                    try:
                        self._callback(url)
                    except Exception:
                        logging.exception('Callback raised an exception, ignoring')
                return super(CallbackRetry, self).increment(method, url, *args, **kwargs)
            
        def retry_callback(url):
            logging.info(f"Retrying, url: {self.url}")

        
        session = session or requests.Session()
        retry = CallbackRetry(
            total=retries,
            read=retries,
            connect=retries,
            status_forcelist=status_forcelist,
            method_whitelist=frozenset(['GET', 'POST']),
            backoff_factor=backoff_factor,
            callback=retry_callback
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session


    def make_request(self, batch_date ):
        logging.info(f"Start getting data for {self.table_name}.")

        pageNum = 1
        rowCount = None
        total_rows = 0

        while True:
            r = self.requests_retry_session().post(url = self.url + self.table_name, 
                                            headers = self.request_headers, 
                                            data = self.get_signed_params(pageNum, batch_date)
                            ) 

            # when no more data has return, break the loop
            if len(r.json()['result']['data']) == 0:
                logging.info(f"no data on page {pageNum}, request finish")
                break
            # check if the request was successful
            if r.json()['isSuccess'] == False:
                raise Exception('Request fail!')
                
            # compare data sample against schema
            if pageNum == 1:
                logging.info("Data Sample:" + json.dumps(r.json()['result']['data'][0]))
                
                miss_columns = [i for i in self.column_list if i not in r.json()['result']['data'][0].keys()]
                extra_columns = [i for i in r.json()['result']['data'][0].keys() if i not in self.column_list]
                if extra_columns != []:
                    logging.warning(f"Extra columns {extra_columns} found")

                if miss_columns != []:
                    logging.warning(f"Columns {miss_columns} missing!")
                    # raise Exception(f"Columns {miss_columns} missing!")

            # check if each query return the same info
            if rowCount != None and rowCount < r.json()['result']['totalCount'] :
                logging.error("total count:" + str(r.json()['result']['totalCount']))
                raise Exception('Query result mismatch!')

            rowCount = r.json()['result']['totalCount']
            logging.info(f"fetching page {pageNum}/{rowCount//self.pageSize+1}, {self.table_name} totalCount: {rowCount}, page size: {self.pageSize}")
            # result += r.json()['result']['data']
            yield r.json()['result']['data']

            total_rows += len(r.json()['result']['data'])
            pageNum += 1
                
        # check if the total number of records match the result
        if total_rows != rowCount and rowCount != None:
            logging.info(f"result set has {total_rows} rows, while totalCount from api has {rowCount}")
            raise Exception('Result count mismatch!')
        
        logging.info(f"Data for {self.table_name} fetched successfully.")
   
    def prepare_file_path(self):
        batch_date_dir = os.path.join(self.save_path, self.file_batch_date_yyyymmdd)
        if ( not os.path.isdir(batch_date_dir ) ):
            os.mkdir(batch_date_dir)
    
    def post_download_data(self, filename):
        # generate the ok file
        ok_filename = filename+".ok"
        if ( os.path.exists(ok_filename) ):
            if( os.path.isfile(ok_filename) ):
                os.remove(ok_filename)
            if( os.path.isdir(ok_filename) ):
                os.removedirs(ok_filename)
        with open(ok_filename, 'w') as fd:
            fd.close()   
   
    def handle_exception(self, filename ):
        os.remove(filename)

    def main(self):
        # format batch date to datetime.datetime
        batch_date = datetime.strptime(self.batch_date_yyyymmdd, "%Y%m%d")
        
        # call the api and get the result, set days_to_load to 0 if all historical data is needed)
        # save the data to local
        self.prepare_file_path()
        filename = os.path.join(self.save_path, self.file_batch_date_yyyymmdd, self.table_name + ".csv.gz")
        try:
            with gzip.GzipFile(filename, 'wb') as compressed:
                with io.TextIOWrapper(compressed, encoding='utf-8', newline='') as wrapper:
                    dict_writer = csv.DictWriter(wrapper, {i:None for i in self.column_list}, extrasaction="ignore")
                    dict_writer.writeheader()
                    for result in self.make_request( batch_date ):
                        dict_writer.writerows(result)
        except Exception as e:
            self.handle_exception(filename)
            raise
               
        logging.info(f"File {filename} saved successfully.")
        self.post_download_data(filename)
          
def start_ingest(entity, myutil, days_to_load = 0, data_day_delta = 1, batch_date = None):
    name_map = {
        "jd_member":'memberInfo', 
        "jd_b2b_order_dtl": 'orderInfo', 
        "jd_pop_consignee":'popConsigneeInfo', 
        "jd_pop_order_dtl":'popDetailInfo', 
        "jd_pop_order":'popInfo'
    }
    url = myutil.get_conf('JDAPI', 'URL')
    signKey = myutil.get_conf('JDAPI', 'SIGNKEY')
    save_path = myutil.get_conf('JDAPI', 'SAVE_PATH')
    sql_dict =  myutil.get_sql_yml_fd(entity)
    columns_list = sql_dict['Staging']['src_columns'].replace(' text','').replace(' ','').split(",")
  
    exactor =  JDextractor(name_map[entity], url, signKey, save_path, columns_list, days_to_load, data_day_delta, batch_date = batch_date)
    exactor.main()

# if __name__ == "__main__":
#     from myutil import Myutil
#     myutil = Myutil('C:\\\\workspace\\project\\LEGO\\code\\airflow-jobs\\dags')
#     start_ingest("jd_member", myutil, days_to_load = 1)
