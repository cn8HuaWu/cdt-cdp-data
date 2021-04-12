from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.acs_exception.exceptions import ClientException
from aliyunsdkcore.auth.credentials import EcsRamRoleCredential
from aliyunsdkkms.request.v20160120.GetSecretValueRequest import GetSecretValueRequest
import json, sys, os
from configparser import ConfigParser

ENV = os.getenv('airflow_env')
def get_pwd(name):
    client = AcsClient(region_id='cn-shanghai', credential=EcsRamRoleCredential("ecs-etl"))
    request = GetSecretValueRequest()
    request.set_accept_format('json')
    request.set_SecretName(name)
    response = client.do_action_with_exception(request)
    # print(str(response, encoding='utf-8'))
    res_json = json.loads(response, encoding='utf8')
    return res_json['SecretData']

def get_rds_conn_str():
    rds_env = ENV + '-RDS'
    rds_host = cp.get(rds_env, 'AIRFLOW_RDS_HOST')
    rds_port = cp.get(rds_env, 'AIRFLOW_RDS_PORT')
    rds_user = cp.get(rds_env, 'AIRFLOW_USER')
    rds_pwd = get_pwd(cp.get(rds_env,'AIRFLOW_PW_KMS'))
    #  postgresql://:@pgm-uf6805zhv5h45yej129250.pg.rds.aliyuncs.com:1433/airflow
    conn_str = "postgresql://{0}:{1}@{2}:{3}/airflow".format(rds_user, rds_pwd, rds_host, rds_port)
    return conn_str

if __name__ == "__main__":
    type = sys.argv[1]
    cp = ConfigParser()
    cp.read( os.path.join("/cdp/airflow/dags/lego/tasks/config/env.conf") )
    if (type == '2'):
        print( get_pwd(cp.get( ENV + '-SMTP', 'PASSWORD_KMS')) , end='')
    elif( type == '1' ):
        print(get_rds_conn_str(), end='')
