import shutil, os , sys
from datetime import datetime
import logging
import imp

# DATASOURCE_ALIAS = {'BU':'input/LEGO', 'DL':"input/LEGO" , 'WC':'weixin/data/weixin', "JD":"input/JD", "OMS":"input/OMS"}
skip_encrypt_file_list = ['cs_mini_member_info']
class Src2stgHandler:
    def __init__(self, 
                level,  
                batch_date, 
                datasource, 
                entity_name,
                stg_suffix, 
                src_filename, 
                myutil,
                ok_file_path,
                has_batchdate = True, 
                src_entity=None ,
                entity_conf=None,
                is_encrypted=False,
                src_aes_key = None,
                src_aes_iv = None,
                has_head = True,
                sheetname = None,
                algo= None,
                excel_skip_row = 0,
                need_encrypt = True,
                overwrite = True,
                output_format='csv',
                output_encoding = 'utf8',
                simple_mode = True,
                read_all = False,
                merge= True,
                excel_fun_list = None,
                **sheet_param
            ):
        self.level = level
        self.batch_date = batch_date
        self.datasource = datasource
        self.entity_name = entity_name
        self.table_suffix = stg_suffix
        self.src_filename = src_filename
        self.has_batchdate = has_batchdate
        self.myutil = myutil
        self.ok_file_path = ok_file_path
        self.src_entity = src_entity
        self.entity_conf = entity_conf
        self.is_encrypted = is_encrypted
        self.src_aes_key = src_aes_key
        self.src_aes_iv = src_aes_iv
        self.has_head = has_head
        self.sheetname = sheetname
        self.algo = algo
        self.excel_skip_row = excel_skip_row
        self.work_data_dir = self.myutil.get_conf('ETL', 'WORK_DATA_DIR')
        self.need_encrypt =  need_encrypt
        self.overwrite = overwrite 
        self.output_format= output_format
        self.output_encoding = output_encoding
        self.simple_mode = simple_mode
        self.read_all = read_all
        self.merge= merge
        self.excel_fun_list = excel_fun_list
        self.sheet_param = sheet_param

    def process_src_data(self):
        ok_dir_path = os.path.dirname(self.ok_file_path)
        ## backup the data file and the ok file
        bucket = self.myutil.get_oss_bucket()
        timestamp_str =  datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        
        #backup ok file
        okfile_basename = os.path.basename(self.ok_file_path)
        backup_path ='/'.join( ('Backup', self.datasource, self.entity_name, self.batch_date, timestamp_str + '_' + okfile_basename) )
        self.myutil.upload_local_oss(bucket, self.ok_file_path, backup_path)
        if (os.path.exists(self.ok_file_path)):
            shutil.move(self.ok_file_path, self.ok_file_path+"_"+datetime.strftime(datetime.now(), '%Y%m%d%H%M%S'))
        #backup data file
        datafile_path = os.path.join(ok_dir_path, "running_" +self.src_filename) 
        backup_path ='/'.join( ('Backup', self.datasource, self.entity_name, self.batch_date, timestamp_str + '_' + self.src_filename) )
        self.myutil.upload_local_oss(bucket, datafile_path, backup_path)

        ## step 1: Clear the local working folder
        entity_data_dir = os.path.join(self.work_data_dir,self.datasource, self.entity_name)
        if( not os.path.isdir(entity_data_dir) ):
            os.makedirs( entity_data_dir )

        # empty the entity folder
        shutil.rmtree(entity_data_dir)
        os.mkdir(entity_data_dir)
        
        ## Step 2: Retrieve Source Data
        src_file_path = datafile_path
       
        ## Step 3: Process Data if needed, eg. unzip/decrypt
        target_file_path = os.path.join(entity_data_dir, self.entity_name + ".csv")
        if( src_file_path.split('.')[-1].lower() == 'gz' ):
            self.myutil.uncompress_gz_file(src_file_path, target_file_path)
        elif( src_file_path.split('.')[-1].lower() in ('xlsx', 'xls') ):
            self.myutil.read_excel_file(src_file_path, target_file_path, skiprow=self.excel_skip_row ,keephead=self.has_head, sheetname_in= self.sheetname)
        else:
            shutil.copyfile( src_file_path, os.path.join(entity_data_dir, target_file_path) )
        
        if ( self.src_entity ):
            sql_dict =  self.myutil.get_sql_yml_fd(self.src_entity)
            columns_list = sql_dict['Staging']['src_columns'].replace(' text','').replace(' ','').split(",")

        ## decrypt the source file under the work dir
        if ( self.src_entity and self.is_encrypted 
            and 'encrypted_columns' in self.entity_conf[self.src_entity] 
            and self.entity_conf is not None):
            logging.info("Start to decrypt the file")
            encrypt_col_list = self.entity_conf[self.src_entity]['encrypted_columns'].split(",")
            self.myutil.decrypt_csv_fields(target_file_path,
                target_file_path,
                0, 
                encrypt_col_list, 
                columns_list, 
                self.src_aes_key, 
                self.src_aes_iv, 
                del_src= True, 
                algo = self.algo )

        ## step 3.5 encrypt the columns
        if (self.src_entity 
            and 'to_encrypt_columns' in self.entity_conf[self.src_entity]  
            and self.entity_conf is not None
            and self.src_entity not in skip_encrypt_file_list 
            and self.need_encrypt):
            logging.info("Start to encrypt the file")
            to_encrypt_list = self.entity_conf[self.src_entity]['to_encrypt_columns'].split(",")
            dl_aes_key = self.myutil.get_dl_aes_key()
            dl_aes_iv = self.myutil.get_dl_aes_iv()
            self.myutil.encrypt_csv_fields(target_file_path, target_file_path,0, to_encrypt_list, columns_list, dl_aes_key, dl_aes_iv, del_src= True, keepheader=self.has_head)
        
        ## Step 5: Upload Data to OSS Staging
        basename = self.entity_name + "_"+ self.table_suffix +".csv" 
        stg_path = '/'.join( (self.level,self.datasource, self.entity_name, self.batch_date, basename) )
        self.myutil.upload_local_oss(bucket, target_file_path, stg_path)

   
    def process_src_data_v2(self, keep_empty= False, merge_source = False):
        excelutils = imp.load_source('excelutils', self.myutil.dag_home+"/tasks/utils/excelutils.py" )
        excel2csv = excelutils.ExcelConverter(keep_empty = keep_empty)
        excel2csv.register_fun_list(self.excel_fun_list) 

        ok_dir_path = os.path.dirname(self.ok_file_path)
        ## backup the data file and the ok file
        bucket = self.myutil.get_oss_bucket()
        timestamp_str =  datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        
        #backup ok file
        okfile_basename = os.path.basename(self.ok_file_path)
        backup_path ='/'.join( ('Backup', self.datasource, self.entity_name, self.batch_date, timestamp_str + '_' + okfile_basename) )
        self.myutil.upload_local_oss(bucket, self.ok_file_path, backup_path)
        if (os.path.exists(self.ok_file_path)):
            shutil.move(self.ok_file_path, self.ok_file_path+"_"+datetime.strftime(datetime.now(), '%Y%m%d%H%M%S'))
        #backup data file
        datafile_path = os.path.join(ok_dir_path, "running_" +self.src_filename) 
        backup_path ='/'.join( ('Backup', self.datasource, self.entity_name, self.batch_date, timestamp_str + '_' + self.src_filename) )
        self.myutil.upload_local_oss(bucket, datafile_path, backup_path)

        ## step 1: Clear the local working folder
        entity_data_dir = os.path.join(self.work_data_dir,self.datasource, self.entity_name)
        if( not os.path.isdir(entity_data_dir) ):
            os.makedirs( entity_data_dir )

        # empty the entity folder
        shutil.rmtree(entity_data_dir)
        os.mkdir(entity_data_dir)
        
        ## Step 2: Retrieve Source Data
        src_file_path = datafile_path

        ## Step 3: Process Data if needed, eg. unzip/decrypt
        target_file_path_list = []
        target_file_path = entity_data_dir
        if( src_file_path.split('.')[-1].lower() == 'gz' ):
            target_unpack_file_path = self.myutil.uncompress_gz_file(src_file_path, os.path.join(target_file_path, self.entity_name) )
            for temp_fn in target_file_path:
                if target_unpack_file_path('.')[-1].lower() in ('xlsx', 'xls') :
                    output_abs_file = excel2csv.convert_xls2csv(temp_fn, 
                        output_path = target_file_path,
                        output_filename = os.path.basename(os.path.splitext(temp_fn)[0]),
                        overwrite = self.overwrite,
                        output_format= self.output_format,
                        output_encoding = self.output_encoding,
                        simple_mode = self.simple_mode,
                        read_all = self.read_all,
                        keep_header=self.has_head,
                        header=self.excel_skip_row,
                        sheet_name= self.sheetname,
                        merge= self.merge,
                        **self.sheet_param
                    )
                    target_file_path_list.extend(output_abs_file)

        elif( src_file_path.split('.')[-1].lower() in ('xlsx', 'xls') ):
            output_abs_file = excel2csv.convert_xls2csv(src_file_path, 
                output_path = target_file_path,
                output_filename = self.entity_name,
                overwrite = self.overwrite,
                output_format= self.output_format,
                output_encoding = self.output_encoding,
                simple_mode = self.simple_mode,
                read_all = self.read_all,
                keep_header=self.has_head,
                header=self.excel_skip_row,
                sheet_name= self.sheetname,
                merge= self.merge,
                **self.sheet_param
            )
            target_file_path_list.extend(output_abs_file)
        else:
            target_file_path = os.path.join(entity_data_dir, self.entity_name + ".csv")
            shutil.copyfile( src_file_path, os.path.join(entity_data_dir, target_file_path) )
            target_file_path_list.append(target_file_path)
        
        if ( self.src_entity ):
            sql_dict =  self.myutil.get_sql_yml_fd(self.src_entity)
            columns_list = sql_dict['Staging']['src_columns'].replace(' text','').replace(' ','').split(",")

            
        for target_file_path in target_file_path_list:
            ## decrypt the source file under the work dir
            if ( self.src_entity and self.is_encrypted 
                and 'encrypted_columns' in self.entity_conf[self.src_entity] 
                and self.entity_conf is not None):
                logging.info("Start to decrypt the file")
                encrypt_col_list = self.entity_conf[self.src_entity]['encrypted_columns'].split(",")
                self.myutil.decrypt_csv_fields(target_file_path,
                    target_file_path,
                    0, 
                    encrypt_col_list, 
                    columns_list, 
                    self.src_aes_key, 
                    self.src_aes_iv, 
                    del_src= True, 
                    algo = self.algo )

            ## step 3.5 encrypt the columns
            if (self.src_entity 
                and 'to_encrypt_columns' in self.entity_conf[self.src_entity]  
                and self.entity_conf is not None
                and self.src_entity not in skip_encrypt_file_list 
                and self.need_encrypt):
                logging.info("Start to encrypt the file")
                to_encrypt_list = self.entity_conf[self.src_entity]['to_encrypt_columns'].split(",")
                dl_aes_key = self.myutil.get_dl_aes_key()
                dl_aes_iv = self.myutil.get_dl_aes_iv()
                self.myutil.encrypt_csv_fields(target_file_path, target_file_path,0, to_encrypt_list, columns_list, dl_aes_key, dl_aes_iv, del_src= True, keepheader=self.has_head)

            ## Step 4: Backup Data to OSS Backup
            # bucket = self.myutil.get_oss_bucket()
            # basename = self.entity_name + "_"+ self.table_suffix +".csv" 
            # timestamp_str =  datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            # backup_path ='/'.join( ('Backup', self.datasource, self.entity_name, self.batch_date, timestamp_str + '_' + basename) )
            # self.myutil.upload_local_oss(bucket, target_file_path, backup_path)
            
            ## Step 5: Upload Data to OSS Staging
            # basename = self.entity_name + "_"+ self.table_suffix +".csv" 
            basename = os.path.basename(target_file_path )
            stg_path = '/'.join( (self.level,self.datasource, self.entity_name, self.batch_date, basename) )
            self.myutil.upload_local_oss(bucket, target_file_path, stg_path)

    def start(self, version = 'v1'):
        if version.lower() == 'v1':
            self.process_src_data()

        if version.lower() == 'v2':
            self.process_src_data_v2()
