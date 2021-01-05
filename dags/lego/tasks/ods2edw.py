import logging

class Ods2edwHandler:
    def __init__(self, 
        batch_date, 
        datasource, 
        entity_name, 
        table_key, 
        table_prefix,
        myutil, 
        db, 
        update_type='F', 
        has_param=False,
        AES_ENCODING='hex',
        AES_KEY=None, 
        DL_AES_KEY=None,
        DL_AES_IV=None,
        ):
        self.batch_date = batch_date
        self.datasource = datasource
        self.entity_name = entity_name
        self.table_key = table_key
        self.table_prefix = table_prefix
        self.myutil = myutil
        self.db = db
        self.update_type = update_type
        self.AES_KEY = AES_KEY
        self.AES_ENCODING = AES_ENCODING
        self.DL_AES_KEY = DL_AES_KEY
        self.DL_AES_IV = DL_AES_IV
        self.has_param = has_param
        self.sql_dict = self.myutil.get_sql_yml_fd( self.datasource.lower() + "_" + self.entity_name )

    def process_ods2edw(self):
        debug_flg = self.myutil.get_conf('ETL','SQL_DEBUG')
        if (debug_flg.upper() == 'TRUE'):
            dgb_ind = True
        else:
            dgb_ind = False
        
        engine = self.db.create_engine(debug_flag = dgb_ind)
        engine = self.db.create_engine()
        conn =  self.db.create_conn( engine )
        var_map = {
            "SRC" : self.datasource,
            "ENTITY": self.entity_name,
            "KEY": self.table_key,
            "batch_date": self.batch_date,
            "EDW_PREFIX": self.table_prefix,
            "AES_KEY": self.AES_KEY,
            "AES_ENCODING": self.AES_ENCODING,
            "DL_AES_KEY": self.DL_AES_KEY,
            "DL_AES_IV": self.DL_AES_IV
        }
        create_table_columns = self.sql_dict['EDW']['create_table_query']
        
        ## Step 1: Build EDW table
        logging.info("Step 1: Build EDW table")
        self.db.execute( create_table_columns, conn )

        ## Step 2: Load ODS table to EDW table
        logging.info("Step 2: Load ODS table to EDW table")
        
        # !!! TODO SCD !!!
        edw_table = 'edw.{EDW_PREFIX}_{SRC}_{ENTITY};'.format_map(var_map)
        logging.info("Load type is %s", self.update_type)
        if( self.update_type.upper() == 'F' ):
            self._full_load(edw_table, conn, var_map)
        elif ( self.update_type.upper() == 'SCD2' ):
            self._scd2_load(edw_table, conn, var_map )
        elif ( self.update_type.upper() == 'SCD2-MD5' ):
            self._scd2_md5_load(edw_table, conn, var_map )
        elif ( self.update_type.upper() == 'SCD3' ):
            self._scd3_load(edw_table, conn, var_map )

        self.db.close_conn(conn)

    def _full_load(self, tablename, conn, params):
        logging.info("delete from table %s and load", tablename)
        trunc_table = 'delete from {0}'.format(tablename)
        self.db.execute( trunc_table, conn )
        if ( self.has_param ):
            load_edw_from_ods = self.sql_dict['EDW']['insert_edw_from_ods_query'].format_map(params)
        else:
            load_edw_from_ods = self.sql_dict['EDW']['insert_edw_from_ods_query']
        self.db.execute( load_edw_from_ods, conn )

    def _scd2_load(self, tablename, conn, params):
        logging.info("SCD2 load table %s ", tablename)
        if ( self.has_param ):
            scd2_update_sql =  self.sql_dict['EDW']['scd2_update_query'].format_map(params)
        else: 
            scd2_update_sql =  self.sql_dict['EDW']['scd2_update_query']
        self.db.execute( scd2_update_sql, conn )
    
    def _scd3_load(self, tablename, conn, params):
        logging.info("SCD3 load table %s ", tablename)
        if ( self.has_param ):
            scd3_update_sql =  self.sql_dict['EDW']['scd3_update_query'].format_map(params)
        else: 
            scd3_update_sql =  self.sql_dict['EDW']['scd3_update_query']
        self.db.execute( scd3_update_sql, conn )
        
    def _scd2_md5_load(self, tablename, conn, params):
        logging.info("SCD2 load table %s ", tablename)
        if ( self.has_param ):
            scd2_update_sql =  self.sql_dict['EDW']['scd2_md5_update_query'].format_map(params)
        else: 
            scd2_update_sql =  self.sql_dict['EDW']['scd2_md5_update_query']
        self.db.execute( scd2_update_sql, conn )

    def start(self):
        self.process_ods2edw()
