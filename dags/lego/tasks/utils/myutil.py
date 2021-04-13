from configparser import ConfigParser
from airflow.utils.email import send_email
import logging, shutil, os
import gzip, re, json
import yaml, csv
import xlrd
import zipfile
import oss2
from oss2.models import PartInfo
from oss2 import determine_part_size
import pandas as pd
import numpy as np
from pandas.errors import EmptyDataError
from Crypto.Cipher import AES
import base64
import chardet
import imp

from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.auth.credentials import EcsRamRoleCredential
from aliyunsdkkms.request.v20160120.CreateSecretRequest import CreateSecretRequest
from aliyunsdkkms.request.v20160120.GetSecretValueRequest import GetSecretValueRequest
import logging, json
from datetime import datetime

from airflow.models import Variable
DAG_HOME =  Variable.get('dag_home').strip().rstrip('/')
mydb = imp.load_source("mydb", DAG_HOME+"/tasks/utils/db.py")
ENV = os.getenv('airflow_env')

class Myutil:
    def __init__(self, dag_home=None, entity_name = None):
        self.cp = ConfigParser()
        self.dag_home = DAG_HOME if dag_home is None else dag_home
        self.db  = None
        self.cp.read( os.path.join(dag_home, "tasks/config/env.conf") )
        cache = imp.load_source("ModifiedProductCache", os.path.join( self.dag_home, "tasks/utils/cache.py") )
        self.productcache = cache.ModifiedProductCache()
        self.entity_name = entity_name
        self.prd_idx_list = None

    def get_secretvalue(self, name, ver = None):
        region_id = self.get_conf('ECSRAM', 'region')
        user = self.get_conf('ECSRAM', 'user')
        acs_client = AcsClient(region_id=region_id, credential=EcsRamRoleCredential(user))
        request = GetSecretValueRequest()
        request.set_accept_format('json')
        request.set_SecretName(name)
        if ver:
            request.set_VersionId(ver)
        try:
            response = acs_client.do_action_with_exception(request)
            res_json = json.loads(response, encoding='utf8')
            return res_json['SecretData']
        except Exception as e:
            logging.exception(e)

    ## read config file
    def get_conf(self, section, option):
        if ENV is not None and ENV != '':
            section = ENV + '-' + section
        if self.cp.has_option(section, option):
            return self.cp.get(section, option)
        elif self.cp.has_option(section, option + "_KMS"):
            return self.get_secretvalue( self.cp.get(section, option + "_KMS"))
        else:
            return None
    ## get DL AES KEY
    def get_dl_aes_key(self):
        return self.get_conf('ETL', 'AES_KEY')
    ## get DL AES IV
    def get_dl_aes_iv(self):
        return self.get_conf('ETL', 'AES_IV')

## OSS
    def get_oss_bucket(self):
        bucket_name = self.get_conf("Aliyun", "OSS_BUCKET")
        OSS_ENDPOINT = self.get_conf("Aliyun", "OSS_ENDPOINT")
        try:
            ALI_ID, ALI_KEY = self.get_conf("Aliyun", "OSS_AUTH").split("/")
            auth = oss2.Auth(ALI_ID, ALI_KEY)
            bucket = oss2.Bucket(auth , OSS_ENDPOINT, bucket_name)
            return bucket
        except Exception as e:
            logging.exception("Failed to fecth the OSS ID/key from the KMS!")
            logging.exception(e)

    ## 只有简单上传， 后续考虑 分片上传，兼容大文件。 >5G
    def upload_local_oss(self, bucket, local_src_path, oss_path):
        logging.info("src_path: %s, tgt_path: %s", local_src_path,oss_path)
        bucket.put_object_from_file(oss_path, local_src_path)

    ## delete single file
    def delete_oss_file_with_name(self, bucket, name):
        bucket.delete_object( name)

    ## delete multiple files
    def delete_oss_file_with_prefix(self, bucket, prefix ):
        logging.info("delete oss files under: %s", prefix)
        for obj in oss2.ObjectIterator(bucket, prefix=prefix):
            self.delete_oss_file_with_name(bucket, obj.key)

    ## copy file in oss
    def copy_files(self, bucket, src_prefix, des_path):
        for obj in oss2.ObjectIterator(bucket, prefix=src_prefix):
            filename = obj.key.split('/')[-1]
            logging.info("copy file: %s to : %s", filename, des_path)
            self.copy_file(bucket, obj.key, des_path + "/" + filename)

    ## multi-part copy, 1M/part
    def copy_file(self, bucket, src, des):
        total_size = bucket.head_object(src).content_length
        part_size = determine_part_size(total_size, preferred_size=1024 * 1024)

        upload_id = bucket.init_multipart_upload(des).upload_id
        parts = []
        part_number = 1

        offset = 0
        while offset < total_size:
            num_to_upload = min(part_size, total_size - offset)
            byte_range = (offset, offset + num_to_upload - 1)

            result = bucket.upload_part_copy(bucket.bucket_name, src, byte_range, des, upload_id, part_number)
            parts.append(PartInfo(part_number, result.etag))

            offset += num_to_upload
            part_number += 1

        bucket.complete_multipart_upload(des, upload_id, parts)

    ## local file
    def modify_ok_file_prefix(self, old_prefix, prefix, ok_file_path):
        source_file_dir = os.path.dirname(ok_file_path)
        source_file_name = os.path.basename(ok_file_path)[:-3]
        if( old_prefix is not None):
            source_file_path = os.path.join(source_file_dir, old_prefix + "_" +source_file_name)
        else:
            source_file_path = ok_file_path[:-3]
        if( not os.path.exists(source_file_path) ):
            source_file_path = os.path.join(source_file_dir, source_file_name)

        done_file_path = os.path.join(source_file_dir, prefix + "_" + source_file_name)
        shutil.move(source_file_path, done_file_path)
        if (prefix == 'failed' and os.path.exists(ok_file_path)):
            shutil.move(ok_file_path, ok_file_path+"_"+datetime.strftime(datetime.now(), '%Y%m%d%H%M%S'))

    def uncompress_gz_file(self, fn_in, fn_out=None, merge= False):
        if merge and fn_out is None:
            raise ValueError("Arguments error: Cannot be fn_out None and merge is False")

        targe_file_list = []
        out_dirname = os.path.dirname(fn_out)
        # out_name = os.path.basename(fn_out).split(".")[0]

        with zipfile.ZipFile(fn_in, 'r') as z:
            # count = 0
            for nm in z.namelist():
                if os.path.exists( os.path.join(out_dirname, nm) ):
                    os.remove(os.path.join(out_dirname, nm))
            z.extractall(path=out_dirname)

            if not merge :
                for nm in z.namelist():
                #     if fn_out is not None:
                #         # new_fn_out = os.path.join(out_dirname,out_name) +"_"+ str(count) + "." + nm.split(".")[-1]
                #         os.rename( os.path.join(out_dirname, nm), new_fn_out)
                #     else:
                    new_fn_out = os.path.join(out_dirname,nm)
                    targe_file_list.append(new_fn_out)
                    # count +=1
            elif merge:
                with open (fn_out , 'wb') as out_fd:
                    for nm in z.namelist():
                        tempfile_path = os.path.join(out_dirname, nm)
                        with open( tempfile_path, 'rb') as ofd:
                            out_fd.write(ofd.read())
                        os.remove(tempfile_path)
                        targe_file_list.append(tempfile_path)
        return targe_file_list

    def compress_file(self, fn_in, fn_out):
        f_in = open(fn_in, 'rb')
        f_out = gzip.open(fn_out, 'wb')
        f_out.writelines(f_in)
        f_out.close()
        f_in.close()

## YML parser
    #  read the DML & DDL
    def get_sql_yml_fd(self, entity):
        yml_file = os.path.join(self.dag_home, "tasks/sqls/%s.yml"%(entity))
        with open( yml_file, 'r', encoding="utf-8") as fd:
            file_data = fd.read()
            sql_dict = yaml.load( file_data, Loader=yaml.FullLoader )
            return sql_dict

    def get_entity_config(self):
        yml_file = os.path.join(self.dag_home, "tasks/config/entity_conf.yml")
        with open( yml_file, 'r', encoding="utf-8") as fd:
            file_data = fd.read()
            conf_dict = yaml.load( file_data, Loader=yaml.FullLoader )
            return conf_dict["entities"]


    ## EXCEL to csv
    # def _read_xls(self, in_xlsx, skiprow, keephead = True ):
    #     wb = xlrd.open_workbook(in_xlsx)
    #     ws = wb.sheet_by_index(0)
    #     skiprow = skiprow if keephead else skiprow + 1
    #     logging.info('row count: ' + str(ws.nrows - skiprow) )
    #     if (skiprow > ws.nrows):
    #         return

    #     col_num = ws.ncols
    #     for nrow in range( ws.nrows ):
    #         row_n = ws.row_values(nrow)

    #         if (''.join(map(lambda x: str(x), row_n)).strip() == ''):
    #             continue

    #         for i in range(col_num):
    #             ctype = ws.cell(nrow, i).ctype
    #             if ( ctype == 0):
    #                 row_n[i] = ''
    #             elif( ctype == 3 ):
    #                 row_n[i] = datetime.strftime(xlrd.xldate.xldate_as_datetime(ws.cell(nrow, i).value, 0), '%Y-%m-%d %H:%M:%S')

    #         yield map(lambda x: str(x).replace('\n', '').replace('\r', '') , row_n)

    # def read_excel_file(self, file_path, out_path, sheetname_in = None, skiprow=0, keephead = True ):
    #     if( os.path.isfile(out_path) ):
    #         os.remove(out_path)

    #     with open(out_path, 'w', encoding='utf-8', newline='') as fd:
    #         csv_writer = csv.writer(fd, delimiter=',',
    #                         quotechar='"', quoting=csv.QUOTE_ALL, escapechar='"' )
    #         csv_writer.writerows( self._read_xls(file_path, skiprow) )

    def read_excel_file(self, file_path, out_path, sheetname_in = None, skiprow=0, keephead = True ):
        if sheetname_in:
            df_made = pd.read_excel(file_path, sheet_name=sheetname_in, encoding='utf8', dtype=str, na_values=' ', skiprows= skiprow,keep_default_na=False)
        else:
            df_made = pd.read_excel(file_path, encoding='utf8', dtype=str, na_values=' ', skiprows= skiprow, keep_default_na=False)
        logging.info('row count: ' + str(len(df_made)))
        df_made = self._format_df(df_made)
        df_made.to_csv(out_path, index=False, escapechar= '"', quoting=csv.QUOTE_ALL,header=keephead )

    def _format_df(self, df_in):
        df_in.replace(to_replace='^\s*$', value= np.nan, regex=True, inplace=True)
        df_in.dropna(axis=0, how= 'all', inplace=True)

    #    df_in = df_in.applymap(str)

        logging.info('write row count: ' + str(len(df_in)))
        df_in.replace(r'[\n|\r|\\]', ' ', regex=True, inplace=True)
        df_in.fillna('', inplace=True)

        column_repaired = []
        for column in df_in.columns:
            column = column.replace('\n', '').replace('\r', '').replace(' ', '_')
            column_repaired.append(column)
        df_in.columns = column_repaired
        return df_in

     ## decrypt by AES-256, kcs5padding
    ## base64
    def decrypt_aes256_ebc(self, content, key, iv = None, mode= AES.MODE_ECB, charset='utf-8'):
        try:
            # logging.info("decrypt key: " + key)
            if (iv):
                generator = AES.new( key.encode(charset), mode, bytes(iv, encoding=charset))
            else:
                generator = AES.new( key.encode(charset), mode)
            decrpyt_bytes = base64.b64decode(content)
            meg = generator.decrypt(decrpyt_bytes)

            return meg[: -1*meg[-1]].decode()
        except:
            logging.exception("Failed to decrypt the content: ", content)
            return content

    ## decrypt by AES-256, kcs5padding
    ## base64
    def decrypt_aes265(self, content, key, iv, mode= AES.MODE_CBC, charset='utf-8'):
        try:
            generator = AES.new( key.encode(charset)  , mode, bytes(iv, encoding=charset))
            decrpyt_bytes = base64.b64decode(content)
            meg = generator.decrypt(decrpyt_bytes)
            return meg[: -1*meg[-1]].decode()
        except:
            logging.exception("Failed to decrypt the content: ", content)
            return content

    ## encrypt by AES-256
    def encrypt_aes256(self, content, key, iv, mode= AES.MODE_CBC, charset='utf-8' ):
        content = ''.join(filter( str.isdigit, content))  ## !!!!! only work for telephone and mobile!!!!!
        if ( content is None or content == ''):
            return content
        bs = 16
        generator = AES.new(key.encode(charset), mode, bytes(iv, encoding=charset))
        PADDING = lambda s: s + (bs - len(s.encode(charset)) % bs) * chr(bs - len(s.encode(charset)) % bs)
        crypt = generator.encrypt(PADDING(content).encode(charset))
        crypted_str = base64.b64encode(crypt)
        result = crypted_str.decode()
        return result

    def  encrypt_filter_value(self, content):
        if ( content is None
            or content.strip() == ''
            or content.strip() == '***********'):
            return False
        else:
            True

    ## encrypt the specified columns
    ## the name/position in the file
    def encrypt_csv_fields(self, file_path, out_path, header, encrypt_col, target_headers, dl_key, dl_iv, del_src=True, keepheader = True, algo='AES-256-CBC' , remove_empty_row=False):
        logging.info("encrypt the file: %s output to %s :",file_path, out_path)
        # shutil.copy(file_path, file_path+"_origin")
        try:
            src_pd = pd.read_csv(file_path, header=header, quotechar='"', delimiter=',', dtype= str)
            input_header= src_pd.columns
            if ( len(target_headers) !=  len(input_header)):
                raise Exception("The header of the input file does not match the target headers: input file: %s", file_path)
        except EmptyDataError:
            logging.error("fiie is empty")
            f = open(out_path,'w')
            f.close()
            return

        if (encrypt_col is None or len(encrypt_col) == 0):
             return

        src_pd.columns = target_headers
        for col in encrypt_col:
            src_pd[col] = src_pd[col].apply(str)
            src_pd[col] = src_pd[col].apply(lambda x: self.encrypt_aes256( x, dl_key, dl_iv ) if ( not self.encrypt_filter_value(x) )  else np.nan)

        if remove_empty_row :
            print("drop empty row")
            src_pd.replace(to_replace='^\s*$', value= np.nan, regex=True, inplace=True)
            src_pd.dropna(axis=0, how= 'all', inplace=True)

        src_pd.fillna('', inplace=True)

        src_pd.to_csv(out_path, header=keepheader, quoting=csv.QUOTE_ALL, quotechar='"', index=False, escapechar='"', encoding='utf-8' )
        if ( file_path.strip() != out_path.strip() and del_src):
            os.remove(file_path)



    ## encrypt the specified columns
    ## the name/position in the file
    def decrypt_csv_fields(self, file_path, out_path, header, encrypt_col, target_headers, dl_key, dl_iv, del_src=True, keepheader = True, algo='AES-256-CBC'):
        def decrypt_func(content, key, iv, algo ):
            if (algo =='AES-256-CBC' ):
                return self.decrypt_aes265(content, key, iv)
            elif (algo =='AES-256-EBC' ):
                return self.decrypt_aes256_ebc(content, key, iv)
            else:
                raise Exception("Does not support the algorithm %s :" , algo)

        logging.info("decrypt the file: %s output to %s :",file_path, out_path)
        src_pd = pd.read_csv(file_path, header=header, quotechar='"', delimiter=',')
        input_header= src_pd.columns
        if ( len(target_headers) !=  len(input_header)):
            raise Exception("The header of the input file does not match the target headers: input file: %s", file_path)
        if (encrypt_col is None or len(encrypt_col) == 0):
             return
        src_pd.fillna('', inplace=True)
        src_pd = src_pd.applymap(str)
        src_pd.columns = target_headers
        for col in encrypt_col:
            src_pd[col] = src_pd[col].apply(lambda x: decrypt_func( x, dl_key, dl_iv, algo ) if ( not pd.isna(x) and x.strip() != '')  else '')

        src_pd.to_csv(out_path, header=keepheader, quoting=csv.QUOTE_ALL, quotechar='"', index=False, escapechar='"', encoding='utf-8' )
        if ( file_path.strip() != out_path.strip() and del_src):
            os.remove(file_path)

    ## decrypt the file
    def decrypt_filed_in_file(self, file_path, out_path, header, col_name, key, IV, dl_key, dl_iv, del_src=True, algo='AES-256-CBC'):
        src_pd = pd.read_csv(file_path, header=header,quotechar='"',delimiter=',')
        src_pd[col_name] = src_pd[col_name].apply(lambda x: self.encrypt_aes256( self.decrypt_aes265(x, key, IV), dl_key, dl_iv ))
        src_pd.to_csv(out_path, header=True, quoting=csv.QUOTE_ALL, quotechar='"', index=False, escapechar='"', encoding='utf-8' )
        if ( file_path.strip() != out_path.strip() and del_src):
            os.remove(file_path)

    def detect_file_encoding(self, file_path):
        with open(file_path,'rb') as fd:
            encode = chardet.detect(fd.read(10000))
            return encode['encoding']

    def convert_file_encode(self, file_path, src_encoding, tgt_encoding = 'utf-8', delete_src = True ):
        parent_charset = {
            "gb2312":"GB18030"
        }
        src_encoding = src_encoding if src_encoding not in parent_charset else parent_charset[src_encoding]

        if ( src_encoding == tgt_encoding ):
            return

        file_out = file_path + "_out"
        i = 0
        while ( os.path.exists(file_out) ):
            i += 1
            file_out = file_path + "_out" + str(i)

        try:
            with open(file_path, 'r',  encoding=src_encoding) as fd:
                with open(file_out, 'w', encoding=tgt_encoding) as wfd:
                    for line in fd.readlines():
                        wfd.write(line.encode(tgt_encoding).decode(tgt_encoding))

            if (delete_src):
                os.remove(file_path)
                os.rename(file_out, file_path)

            return file_out
        except Exception as e:
            logging.exception(e)

    ## in case we need the sftp client
    # def sftp_download_file(self, path, name):
    #     h = self.get_conf('SFTP', 'SFTP_HOST')
    #     p = self.get_conf('SFTP', 'SFTP_PORT')
    #     u = self.get_conf('SFTP', 'SFTP_USER')
    #     pkey = self.get_conf('SFTP', 'SFTP_PRIVATE_KEY')
    #     cnopts = pysftp.CnOpts()
    #     cnopts.hostkeys = None
    #     with pysftp.Connection(h, username=u, private_key=pkey,cnopts=cnopts, log=True) as sftp:
    #         with sftp.cd(path):           # temporarily chdir to allcode
    #             sftp.get(name)         # get a remote file


    def send_success_mail(self, kwargs):
        logging.info('sending out the success mail')
        #default_args:{'owner': 'cdp_admin', 'email': ['zhxie@deloitte.com.cn'],
        # #'email_on_failure': True, 'email_on_retry': False,
        # 'depends_on_past': False, 'start_date': datetime.datetime(2020, 4, 1, 0, 0, tzinfo=<TimezoneInfo [UTC, GMT, +00:00:00, STD]>),
        # 'max_active_runs': 1}
        default_subject = 'Success'
        default_html_content = (
            'success mail.'
        )
        mail_to = 'zhxie@deloitte.com.cn'
        send_email(mail_to, default_subject, default_html_content)

    def count_csvfile_line(self, path, has_head):
        fct = -1 if has_head else 0
        if (path.split('.')[-1].lower() in ('csv', 'txt') ):
            with open(path, "rb") as fd:
                for count, item in enumerate(fd):
                    pass
                fct += count+1
        elif (path.split('.')[-1].lower() in ('xlsx', 'xls')):
            data=xlrd.open_workbook(path)
            table=data.sheets()[0]
            fct += table.nrows

        logging.info("source file lines is " + str(fct))
        return fct if fct >0 else 0


    def init_db(self):
        gp_host = self.get_conf( 'Greenplum', 'GP_HOST')
        gp_port = self.get_conf( 'Greenplum', 'GP_PORT')
        gp_db = self.get_conf( 'Greenplum', 'GP_DB')
        gp_usr = self.get_conf( 'Greenplum', 'GP_USER')
        gp_pw = self.get_conf( 'Greenplum', 'GP_PASSWORD')
        self.db = mydb.Mydb(gp_host, gp_port, gp_db, gp_usr, gp_pw)

    def get_db(self):
        if self.db is None:
            self.init_db()
        return self.db

    def get_modified_productcache(self):
        if not self.productcache.is_initialized():
            if self.db is None:
                self.init_db()

            cache_query = "select * from edw.d_dl_modified_product"
            logging.info("Initializate the product cache")
            with self.db.create_session() as session:
                tainedproducts = session.execute(cache_query)
                for product in tainedproducts:
                    self.productcache.add(product)
        return self.productcache

    def gen_cache_key(self, row):
        if self.entity_name is None:
            return row[0]

        if self.prd_idx_list is None:
            entity_cfg = self.get_entity_config()[self.entity_name]
            logging.info("product cache code list:" + entity_cfg["productcode_index"] )
            self.prd_idx_list = entity_cfg["productcode_index"]
        return self.prd_idx_list
        # #     self.prd_idx_list = list(sorted(str(entity_cfg["productcode_index"]).split(",")))
        # #     if self.prd_idx_list is not None and  int(self.prd_idx_list[0]) < 0 or int(self.prd_idx_list[-1]) > len(row):
        # #         logging.warning("modified product code index is incorrect, it's >len(list) or <0 ")
        # #         self.prd_idx_list = [0]

        # # key = "_".join( row[int(i)] for i in self.prd_idx_list )
        # return key

    # need add key column index
    def filter_modified_product(self, row:list, *args):
        productcache =  self.get_modified_productcache()
        productkey = self.gen_cache_key(row)
        logging.info("product key " +  str(productkey))
        rs = productcache.search(row[int(productkey)])
        if rs is not None:
            if rs.action_flag.lower() == 'delete':
                return None
            elif rs.action_flag.lower() == 'update':
                row[int(productkey)] = rs.should_be_sku_id
        return row

    def rearrange_columns(self, row:list, input_file_path, sheetname = None, *args):
        if row is None:
            return row
        file_name = os.path.basename(input_file_path)
        def new_column(idx_chr):
            if (idx_chr == '-'):
                return ''
            elif( idx_chr == '/' ):
                return None
            elif( idx_chr.isdigit() ):
                return row[int(idx_chr)]
        # 'all_reg': '',

        # logging.warning("entity name: " + self.entity_name)
        # elf._entity_cfg = None
        if not hasattr(self, "_entity_cfg") or  self._entity_cfg is None or self._input_file_path != input_file_path:
            self._entity_cfg = self.get_entity_config()[self.entity_name]
            self._sortlist = None
            self._input_file_path = input_file_path
            if "column_positions" in self._entity_cfg:
                filename_col_reg = self._entity_cfg["column_positions"]
            # filename_col_reg = '{"ABC":{"filename":"cal*","sheets": [["blc", "0,1,3,2,-"]]}}'
            aj = json.loads(filename_col_reg)
            for k in aj.keys():
                cl = dict(aj[k])
                filename_reg = cl['filename']
                if re.match(filename_reg, file_name):
                    if 'all_reg' in cl:
                        self._sortlist = cl['all_reg']
                        break

                    if 'sheets' in cl and sheetname is not None:
                        for tmpsn, sl in cl['sheets']:
                            if tmpsn == sheetname:
                                sortlist = sl
                                break

                    if self._sortlist is not None:
                        break

        if self._sortlist is None:
            return row
        else:
            new_row = []
            for inx in self._sortlist.split(","):
                col =  new_column(inx)
                if col is not None:
                    new_row.append(col)

            # new_row = [  ]
            # new_row = list(filter(lambda x: x is not None, new_row))
            return new_row

    def send_failure_mail(self):
        pass

if __name__ == "__main__":
    pass
    # myutil = Myutil('/cdp/airflow/dags/lego')
    # dl_aes_key = myutil.get_conf('ETL', 'AES_KEY')
    # dl_aes_iv = myutil.get_conf('ETL', 'AES_IV')
    # sql_dict =  myutil.get_sql_yml_fd('jd_pop_consignee')
    # to_list = ['telephone','mobile']
    # columns_list = sql_dict['Staging']['src_columns'].replace(' text','').replace(' ','').split(",")
    # print(columns_list)
    # input_file = '/root/zhenmao/popConsigneeInfo.csv'
    # output_path = '/root/zhenmao/popConsigneeInfo.csv'
    # myutil.encrypt_csv_fields(input_file, output_path,0, to_list, columns_list, dl_aes_key, dl_aes_iv, del_src= True)

#    conf = myutil.get_sql_yml_fd('jd_b2b_order_dtl')
#
#  = conf['Staging']['src_columns'].replace(' text','').replace(' ','').split(",")
#    print(columns)
#    print(conf['Staging']['src_columns'])
#    myutil.read_excel_file("C:\workspace\project\LEGO\dev\data\product_info\product_clean_20200415.xlsx", "C:\workspace\project\LEGO\dev\data\product_info\product_clean_20200415.csv")
#    key =''
#    offsalt = ''
#    myutil.decrypt_filed_in_file('test.csv','test.csv', 0, 'mobile', key, offsalt)

#    sql_dict = myutil.get_sql_yml_fd("member")
#    print(sql_dict['EDW']["scd2_update_query"])
