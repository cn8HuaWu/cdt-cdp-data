import logging
from datetime import datetime,timedelta
from pytz import timezone

class Stg2odsHandler:
    def __init__(self, temp_folder, stg_level, ods_level, batch_date, datasource, entity_name, suffix, table_key, myutil, db, has_head = 1):
        self.temp_folder = temp_folder
        self.stg_level = stg_level
        self.ods_level = ods_level
        self.batch_date = batch_date
        self.datasource = datasource
        self.entity_name = entity_name
        self.suffix = suffix
        self.table_key = table_key
        self.myutil = myutil
        self.db = db
        self.SRC_FILE_HAS_HEADER = has_head
        self.work_data_dir = self.myutil.get_conf('ETL', 'WORK_DATA_DIR')
        # self.datahub_stg_tables = ['consumer_info','return_order','return_order_dtl', 'order', 'order_dtl']
        self.datahub_stg_tables = {'return_order':'return_order_header',
                                'return_order_dtl':'return_order_item', 
                                'order':'order_header', 
                                'order_dtl':'order_item',
                                'consumer_info':'consumer_info',
                                'store_order':'store_order',
                                'store_order_dtl':'store_order_dtl',
                                'store_order_payment_dtl':'store_order_payment_dtl',
                                'store_order_coupon_dtl':'store_order_coupon_dtl',
                                'store_return_order':'store_return_order',
                                'store_return_order_dtl':'store_return_order_dtl',
                                'store':'store',
                                'store_warehouse_inventory':'store_warehouse_inventory',
                                'store_inventory':'store_inventory',
                                'store_traffic':'store_traffic',
                                'store_member':'store_member',
                                'store_member_point':'store_member_point',
                                'store_point_transaction':'store_point_transaction',
                                'store_coupon':'store_coupon',
                                'income_expense':'income_expense',
                                'income_statement':'income_statement',
                                'expense_statement':'expense_statement'
                            }
        self.datahub_intime_table = {
            'consumer_info':'consumer_info',
            'income_expense':'income_expense',
            'income_statement':'income_statement',
            'expense_statement':'expense_statement'
        }   
    
    def _is_table_exists(self, schema, tablename, conn):
        query = "select  from information_schema.tables where table_schema='{0}' and table_name='{1}'".format(schema, tablename)
        res = self.db.execute(query, conn)
        return True if res.rowcount>0 else False

    def process_stg2ods(self):
        sql_dict = self.myutil.get_sql_yml_fd( self.datasource.lower() + "_" + self.entity_name )
        ## Step 2: Build Staging table
        src_columns = sql_dict["Staging"]["src_columns"]
        # HEADER = "HEADER" if self.SRC_FILE_HAS_HEADER == 1 else ""
        HEADER = "true" if self.SRC_FILE_HAS_HEADER == 1 else "false"

        ks = self.table_key.split(",") 
        joins = 'on ODS.{0} = STG.{1} '.format(ks[0],ks[0])
        conditions = 'where STG.{0} is null '.format(ks[0])
        for k in ks[1:]:
            joins = joins + ' and ODS.{0} = STG.{1} '.format(k,k)
            conditions =  conditions + ' and STG.{0} is null '.format(k)

        filter =  joins +  conditions  
        ali_id, ali_key = self.myutil.get_conf("Aliyun", "OSS_AUTH").split("/")
        var_map = {
            "SRC" : self.datasource,
            "ENTITY": self.entity_name,
            "OSS_PREFIX": self.datahub_stg_tables[self.entity_name] if self.entity_name in self.datahub_stg_tables else '',
            "DDL_COLUMNS": src_columns,
            "OSS_ENDPOINT": self.myutil.get_conf( 'Aliyun', 'OSS_ENDPOINT' ),
            "ALI_ID": ali_id,
            "ALI_KEY": ali_key,
            "OSS_BUCKET": self.myutil.get_conf('Aliyun', 'OSS_BUCKET' ),
            "HEADER": HEADER,
            "FILTER": filter,
            "BATCH_DATE": self.batch_date,
            "SUFFIX": self.suffix
        }
        
        debug_flg = self.myutil.get_conf('ETL','SQL_DEBUG')
        if (debug_flg.upper() == 'TRUE'):
            dgb_ind = True
        else:
            dgb_ind = False
        
        engine = self.db.create_engine(debug_flag = dgb_ind)
        conn =  self.db.create_conn( engine )
        # build_drop_stg_table='drop EXTERNAL table if exists STG.R_{SRC}_{ENTITY}_{SUFFIX};'.format_map(var_map)
        build_drop_stg_table='drop FOREIGN TABLE if exists STG.R_{SRC}_{ENTITY}_{SUFFIX};'.format_map(var_map)
        self.db.execute( build_drop_stg_table, conn )
        # stg_table = 'R_{SRC}_{ENTITY}_{SUFFIX}'.format_map(var_map)
        # if self._is_table_exists("STG", stg_table, conn):
        # if (self.entity_name == 'consumer_info'):
        if (self.entity_name in self.datahub_stg_tables) :
            # Assume all the data send into Datahub can be consumed in half hour
            # if run time after 00:30, then load data income on currend day
            # if run time before 00:30, then load data income at yesterday
            run_hour = datetime.now(timezone('Asia/Shanghai')).hour
            run_minuter = datetime.now(timezone('Asia/Shanghai')).minute
            if run_hour == 0 and run_minuter <= 30 and self.entity_name in self.datahub_intime_table:

                datahub_run_date = datetime.strftime( datetime.now(timezone('Asia/Shanghai')) - timedelta(1) ,'%Y%m%d')
            else:
                datahub_run_date = ''
            #    datahub_run_date = datetime.strftime( datetime.now(timezone('Asia/Shanghai')) ,'%Y%m%d')
            var_map['DATAHUB_RUN_DATE'] = datahub_run_date
            build_stg_table_srcipt = '''
                CREATE FOREIGN TABLE STG.R_{SRC}_{ENTITY}_{SUFFIX}(
                    {DDL_COLUMNS}
                )
                server cdp_oss_server
                options (
                        prefix 'Staging/{SRC}/{OSS_PREFIX}/{DATAHUB_RUN_DATE}',
                        format 'csv',
                        delimiter ',',
                        header '{HEADER}',
                        quote '"',
                        escape '"',
                        encoding 'UTF-8'
                );
            '''.format_map(var_map)
            self.db.execute( build_stg_table_srcipt, conn )
        else:
            build_stg_table_srcipt = '''
                CREATE FOREIGN TABLE STG.R_{SRC}_{ENTITY}_{SUFFIX}(
                    {DDL_COLUMNS}
                )
                server cdp_oss_server
                options (
                        prefix 'Staging/{SRC}/{ENTITY}/{BATCH_DATE}/',
                        format 'csv',
                        delimiter ',',
                        header '{HEADER}',
                        quote '"',
                        escape '"',
                        encoding 'UTF-8'
                );
            '''.format_map(var_map)
            self.db.execute( build_stg_table_srcipt, conn )

        ## Step 3: Build ODS table
        drop_ext_table = 'drop FOREIGN table if exists ODS.R_{SRC}_{ENTITY};'.format_map(var_map)
        self.db.execute( drop_ext_table, conn )
        # ods_table = 'R_{SRC}_{ENTITY}'.format_map(var_map)
        # if self._is_table_exists("ODS", ods_table, conn):
        build_ods_table_script = '''
            CREATE FOREIGN TABLE ODS.R_{SRC}_{ENTITY}(
                {DDL_COLUMNS},
                DL_BATCH_DATE varchar(8),
                DL_LOAD_TIME timestamp
            )
            server cdp_oss_server
                options (
                        prefix 'ODS/{SRC}/{ENTITY}/',
                        format 'csv',
                        delimiter ',',
                        header 'false',
                        quote '"',
                        escape '"',
                        encoding 'UTF-8'
                );
        '''.format_map(var_map)
        self.db.execute( build_ods_table_script, conn )

        ## Step 4: Clean the temporary output folder
        bucket = self.myutil.get_oss_bucket()
        prefix = '/'.join((self.temp_folder , "ODS_WTB" , self.datasource, self.entity_name)) + '/'
        self.myutil.delete_oss_file_with_prefix( bucket, prefix)

        ## Step 5: Build writable table
        drop_wtb_ext_table = 'drop EXTERNAL table if exists WTB.R_{SRC}_{ENTITY};'.format_map(var_map)
        self.db.execute( drop_wtb_ext_table, conn )
        # wtb_table = 'R_{SRC}_{ENTITY}'.format_map(var_map)
        # if self._is_table_exists("WTB", wtb_table, conn):
        build_wtb_table_scripts = '''
            CREATE WRITABLE EXTERNAL TABLE WTB.R_{SRC}_{ENTITY}(
                {DDL_COLUMNS},
                DL_BATCH_DATE varchar(8),
                DL_LOAD_TIME timestamp
            )
            LOCATION ('oss://{OSS_ENDPOINT} 
                    prefix=Temp/ODS_WTB/{SRC}/{ENTITY}/
                    id={ALI_ID} key={ALI_KEY} 
                    bucket={OSS_BUCKET}
                    async=true')
            FORMAT 'CSV' ( QUOTE '"' ESCAPE '"' )
            ENCODING 'UTF-8' 
            ;
        '''.format_map(var_map)
        self.db.execute( build_wtb_table_scripts, conn )

        ## Step 6: Merge data into ODS
        build_temp_table_scripts = '''
            --Merge by key
            create temporary table t1 as 
            select ODS.* 
                from ODS.R_{SRC}_{ENTITY} ods
            left join STG.R_{SRC}_{ENTITY}_{SUFFIX} stg 
            {FILTER}
            ;
        '''.format_map(var_map)
        self.db.execute( build_temp_table_scripts, conn )

        build_insert_wtb_table_scripts ='''
            insert into WTB.R_{SRC}_{ENTITY}
            select * 
            from t1
            union all
            select *,
                '{BATCH_DATE}' DL_BATCH_DATE,
                current_timestamp DL_LOAD_TIME
            from STG.R_{SRC}_{ENTITY}_{SUFFIX}
            ;
            '''.format_map(var_map)
        self.db.execute( build_insert_wtb_table_scripts, conn )

        ## Step 7: Update OSS file
        ods_entity_prefix =  '/'.join((self.ods_level, self.datasource, self.entity_name))+"/"
        logging.info( 'Delete ODS table files: %s',ods_entity_prefix )
        self.myutil.delete_oss_file_with_prefix( bucket, ods_entity_prefix )

        tmp_files_oss = "/".join((self.temp_folder,"ODS_WTB", self.datasource, self.entity_name))+"/"
        ods_table_oss = "/".join((self.ods_level, self.datasource, self.entity_name))
        logging.info("copy file from temp folder to ods foler....")
        logging.info("temp path: %s, ods path: %s", tmp_files_oss,  ods_table_oss)
        self.myutil.copy_files(bucket, tmp_files_oss, ods_table_oss )
        self.db.close_conn(conn)

    def start( self ):
        self.process_stg2ods()