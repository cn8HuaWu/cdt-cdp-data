# 以下4个配置信息需要联系OSS 管理员预先配置
AccessID = ''  # OSS登录名
AccessKey = '' #OSS密码
OSS_ENDPOINT = 'https://oss-cn-shanghai.aliyuncs.com' # OSS host
bucket_name = 'cdp-data-rd-dev' # oss bucket name

# 以下3个配置，根据需要修改
DEFAULT_SIZE_PER_PART = 1024 #kb # 当文件>1MB的时候，分片上传，每次传1M
LOG_NAME = 'wechat_historical_oss' #在脚本运行的目录底下生成的日志名字
OSS_PATH = 'wechat/' #上传到OSS的路径
CREATE_FOLDER_IND = True
APPEND_TIMESTAMP_IND = True

import os
from oss2 import SizedFileAdapter
import oss2
from oss2.models import PartInfo
from collections import namedtuple
import hashlib
import math, datetime
import time
import re

INIT = "init"
UPLOADING = 'uploading'
FAILED = 'failed'
SUCCESS = 'success'
SIMPLE_UPLOAD = 'simple'
MUTILPART_UPLOAD = 'part_upload'
TIME_FORMAT='%Y-%m-%d %H:%M:%S.%f'
LOG_TIME_FORMAT='%Y-%m-%d-%H:%M:%S'

partStatus = namedtuple("partStatus", "n, start, end")
class OSSlog():
    def __init__(self, name, path,  checksum, size, partSize, uploadType, partCount,status,
                osspath = None, uploadId=None,
                completedParts=None, partStatus:list=None ) -> None:
        self.name  = name
        self.path = path
        self.osspath = osspath
        self.checksum = checksum
        self.size = size
        self.partSize = partSize
        self.uploadType = uploadType
        self.partCount = partCount
        self.status = status
        self.uploadId=uploadId
        self.completedParts=completedParts
        self.partStatus=partStatus if partStatus is not None else []
        self.parts = None

    def __eq__(self, s):
        if self.name == s.name \
            and self.path == s.path \
            and self.osspath == s.osspath \
            and self.checksum == s.checksum:
            return True
        else:
            return False

    def __str__(self) -> str:
        formatstr = 'name: {0}\npath: {1}\noss path: {2}\nchecksum: {3}\nsize: {4}\npart size: {5}\nupload type: {6}\nupload id: {7}\npart count: {8}' \
            .format(self.name, self.path, '' if self.osspath is None else self.osspath, self.checksum, self.size, self.partSize, self.uploadType, '' if self.uploadId is None else self.uploadId , self.partCount)
        if self.partStatus is not None and len(self.partStatus) > 0:
            substatus_list = ["part num: {0}, {1} ~ {2}".format( s.n, s.start, s.end ) for s in self.partStatus ]
            partnum_str = "\n".join(substatus_list)
        else:
            partnum_str = "part num: /"

        status_str = "status:" if self.status is None  else "status: " + self.status

        return  "\n".join([formatstr, partnum_str, status_str])

    def set_uploaded_parts(self, parts:list):
        self.parts = parts

    def get_uploaded_parts(self):
        if self.parts is None:
            return []
        return self.parts

def gen_osslog(file_path):
    name = os.path.basename(file_path)
    path = os.path.dirname(file_path)
    checksum = md5(file_path)
    size = get_filesize(file_path)
    partSize = DEFAULT_SIZE_PER_PART*1024
    uploadtype =  SIMPLE_UPLOAD if int(size) <= DEFAULT_SIZE_PER_PART*1024 else MUTILPART_UPLOAD
    partCount = math.ceil(size/partSize)
    return OSSlog(name=name, path=path,
        checksum=checksum, size=size,
        partSize=partSize, uploadType=uploadtype,
        partCount=partCount,
        status= INIT, osspath= OSS_PATH)

# kb
def get_filesize(filePath):
    fsize = os.path.getsize(filePath)
    return fsize

#计算文件的MD5
def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def oss_has_permission(bucket, oss_path):
    result = bucket.get_object_acl(oss_path)
    print('get object acl :', result.acl)

def get_oss_bucket(AccessID, AccessKey, bucket_name):
    try:
        auth = oss2.Auth(AccessID, AccessKey)
        bucket = oss2.Bucket(auth , OSS_ENDPOINT, bucket_name)
        return bucket
    except Exception as e:
        print(e)

def simple_upload_oss( bucket, local_file_path, oss_path):
    # print("src_path: %s, tgt_path: %s", local_file_path,oss_path)
    bucket.put_object_from_file(oss_path, local_file_path)

def mutilple_upload_oss(bucket, local_file_path, osslog:OSSlog, oss_path, part_number = 1):
    part_size = osslog.partSize
    total_size = osslog.size
    if part_number > 1 and osslog.uploadId is not None:
        upload_id = osslog.uploadId
    else:
        upload_id = bucket.init_multipart_upload(oss_path).upload_id
        osslog.uploadId = upload_id
    parts = osslog.get_uploaded_parts()
    try:
        with open( os.path.join( osslog.path, osslog.name) , 'rb') as fileobj:
            part_number = part_number
            offset = (part_number-1) *int(part_size)
            while offset < int(osslog.size):
                # if part_number == 3:
                #     raise ValueError("failed the process")

                start = datetime.datetime.now().strftime(TIME_FORMAT)
                num_to_upload = min(int(part_size), int(total_size) - int(offset))
                # 调用SizedFileAdapter(fileobj, size)方法会生成一个新的文件对象，重新计算起始追加位置。
                result = bucket.upload_part(oss_path, upload_id, part_number,
                                            SizedFileAdapter(fileobj, num_to_upload))
                parts.append(PartInfo(part_number, result.etag))
                end = datetime.datetime.now().strftime(TIME_FORMAT)
                partinfo = partStatus(part_number, start, end)
                osslog.partStatus.append(partinfo)
                offset += num_to_upload
                part_number += 1
        bucket.complete_multipart_upload(oss_path, upload_id, parts)
        osslog.status = SUCCESS
    except Exception as e:
        print(e)
        osslog.status = FAILED
    return osslog

def  resume_mutilple_upload_oss(bucket, osslog:OSSlog, oss_path):
    parts_info = [ part for part in oss2.PartIterator(bucket, oss_path, osslog.uploadId)]
    osslog.set_uploaded_parts(parts_info)
    num_list = [part_info.part_number for part_info in parts_info]
    return max(num_list)


def upload_file_by_osslog(osslog:OSSlog, create_folder= False, ts_suffix = False, timestamp_str = None):
    bucket = get_oss_bucket(AccessID, AccessKey, bucket_name)

    src_path = os.path.join(osslog.path, osslog.name)
    print("Start to upload " + src_path)

    full_path_name = osslog.name
    if create_folder:
        folder_name = ''.join(os.path.splitext(osslog.name)[:-1])
        full_path_name = folder_name + '/' + osslog.name

    if ts_suffix and timestamp_str is not None:
        full_path_name  = full_path_name + '_' + timestamp_str

    target_path = os.path.join(osslog.osspath, full_path_name)

    if osslog.uploadType == SIMPLE_UPLOAD:
        try:
            simple_upload_oss(bucket, src_path,  target_path)
            osslog.status = SUCCESS
        except:
            osslog.status = FAILED

    elif osslog.uploadType == MUTILPART_UPLOAD:
        if osslog.status == INIT:
            osslog = mutilple_upload_oss(bucket, src_path, osslog, target_path)

        elif osslog.status == FAILED:
            maxnum = resume_mutilple_upload_oss(bucket, osslog, target_path)
            osslog = mutilple_upload_oss(bucket, src_path, osslog, target_path, part_number=maxnum +1)
    return osslog

def read_log_status(log_path) ->list:
    oss_list = []
    if not os.path.exists(log_path):
        return oss_list
    with open(log_path,'r' ) as fd:
        str_list = []
        for line in fd.readlines():
            line = line.strip(" ").strip("\n").strip("\r")
            if line == '[start]':
                log_name = None
                log_path = None
                log_checksum = None
                log_size = None
                log_partSize= None
                log_uploadType = None
                log_partCount = None
                log_status = None
            elif line == '[end]':
                for attr  in str_list:
                    if "name" in attr and attr.index('name:') ==0:
                        log_name = ":".join(attr.split(":")[1:]).strip(" ")

                    if "path" in attr and  attr.index('path:')==0:
                        log_path = ":".join(attr.split(":")[1:]).strip(" ")

                    if "checksum" in attr and attr.index('checksum:')==0:
                        log_checksum = ":".join(attr.split(":")[1:]).strip(" ")

                    if "oss path" in attr and attr.index('oss path:')==0:
                        log_osspath = ":".join(attr.split(":")[1:]).strip(" ")

                    if "size" in attr and attr.index('size:')==0:
                        log_size = ":".join(attr.split(":")[1:]).strip(" ")

                    if "part size" in attr and attr.index('part size:')==0:
                        log_partSize = ":".join(attr.split(":")[1:]).strip(" ")

                    if "upload type" in attr and attr.index('upload type:')==0:
                        log_uploadType = ":".join(attr.split(":")[1:]).strip(" ")

                    if "part count" in attr and attr.index('part count:')==0:
                        log_partCount = ":".join(attr.split(":")[1:]).strip(" ")

                    if "status" in attr and attr.index('status:')==0:
                        log_status = ":".join(attr.split(":")[1:]).strip(" ")

                    if "upload id" in attr and attr.index('upload id:')==0:
                        log_uploadid = ":".join(attr.split(":")[1:]).strip(" ")

                if all([log_name,log_path,log_checksum,log_size,log_partSize,log_uploadType,log_partCount,log_status]):
                    osslog = OSSlog(log_name, log_path, log_checksum, log_size, log_partSize, log_uploadType, log_partCount, log_status, uploadId=log_uploadid, osspath=log_osspath)
                    # print(str(osslog))
                    if osslog in oss_list:
                        oss_list.remove(osslog)
                    oss_list.append(osslog)


                str_list.clear()
                log_name = None
                log_path = None
                log_checksum = None
                log_size = None
                log_partSize= None
                log_uploadType = None
                log_partCount = None
                log_status = None
            elif line != '':
                str_list.append(line)
    return oss_list

def merge_osslog(new_log_list:list, exist_log_list:list) -> list:
    if exist_log_list is None:
        return new_log_list

    rm_index_list = []
    log_len = len(new_log_list)
    new_log_list.extend(exist_log_list)

    for i in range(0, log_len):
        for j in range(0, len(exist_log_list)):
            if exist_log_list[j] == new_log_list[i]:
                rm_index_list.append(i)
                # new_log_list.append(exist_log_list[i])
                # if  exist_log_list[j].status == FAILED:

                break

    for i in sorted(rm_index_list, reverse=True):
        new_log_list.pop(i)


    return new_log_list

def upload_log_file(logfile):
    bucket = get_oss_bucket(AccessID, AccessKey, bucket_name)
    simple_upload_oss(bucket, logfile, os.path.join(OSS_PATH, os.path.basename(logfile) + "_" + str(int( time.mktime(datetime.datetime.now().timetuple())))))


def backup_log_file(logfile):
    os.rename(logfile, logfile + "_" + str(int( time.mktime(datetime.datetime.now().timetuple()))) )

# 只遍历当前文件，且只上传: txt, csv, xls, xlsx, dat后缀名的文件
def start():

    # 1. 查找当前目录，是否有log文件
    # 2. 如有log文件，根据log判断是否需要重新上传/续传的文件
    # 3. 根据step 2的结果，开始挨个文件开始上传. 上传过程先写日志，再上传数据文件
    # 4. 覆盖原有日志
    # 5. 上传日志文件
    cmd = os.getcwd()
    timestamp_str = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d%H%M%S')

    # cmd =r'C:\workspace\project\LEGO\code\CDP-CR\POC\migration'
    log_exists_file = ''
    new_osslog_list = []
    for root, dirs, filenames in os.walk(cmd, topdown=False):
        for filename in filenames:
            if LOG_NAME == os.path.splitext(filename)[0].lower() :
                log_exists_file = filename

            name_no_ext = ''.join(os.path.splitext(filename)[:-1])
            # print(name_no_ext)
            if re.match('^\w*$', name_no_ext) is None:
                print("WARNING: 文件名含有特殊字符," + filename)
                continue
            if os.path.splitext(filename)[-1] in (".txt", ".csv", ".dat"):
                tmp_osslog = gen_osslog(os.path.join(root,filename))
                new_osslog_list.append(tmp_osslog)

    log_path = os.path.join(cmd, LOG_NAME+".log")

    if os.path.exists(log_path) :
        exist_log_list= read_log_status(log_path)
        backup_log_file(log_path)
        to_process_oss = merge_osslog(new_osslog_list, exist_log_list)
    else:
        to_process_oss = new_osslog_list


    with open(log_path, 'w') as fd:
        for lg in to_process_oss:
            fd.write("[start]\n")
            if lg.status == SUCCESS:
                osslog = lg
            else:
                osslog = upload_file_by_osslog(lg, CREATE_FOLDER_IND, APPEND_TIMESTAMP_IND, timestamp_str = timestamp_str)
            fd.write(str(osslog))
            fd.write("\n[end]\n\n")

    upload_log_file(log_path)


if __name__ == "__main__":
    # # local_file_path = r'C:\workspace\project\LEGO\code\CDP-CR\POC\calendar - Copy.xlsx'
    # log  = gen_osslog(local_file_path)
    # print(log)
    start()
