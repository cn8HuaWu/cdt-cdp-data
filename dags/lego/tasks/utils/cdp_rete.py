from collections import defaultdict
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text
from airflow.models import Variable
import functools
from typing import List
# from .myutil import Myutil
import keyword
import re
from myutil import Myutil
# import imp

DAG_HOME =  Variable.get('dag_home').strip().rstrip('/')
# mydb = imp.load_source("mydb", DAG_HOME+"/tasks/utils/db.py")
# utiltools = imp.load_source("myutil", DAG_HOME+"/tasks/utils/myutil.py")

SQL_TYPE = 'SQL'
EXP_TYPE = 'EXP'

ERROR_LEVEL_FATAL = 'ERROR'
ERROR_LEVEL_WARNING = 'WARNING'

OPERATOR_STATE_SKIP = "skip"
OPERATOR_STATE_FALSE = "false"
OPERATOR_STATE_TRUE = "true"
OPERATOR_STATE_CHILD_FALSE = "childfalse"


myutil = Myutil(DAG_HOME)
db = myutil.get_db()
# gp_host = myutil.get_conf( 'Greenplum', 'GP_HOST')
# gp_port = myutil.get_conf( 'Greenplum', 'GP_PORT')
# gp_db = myutil.get_conf( 'Greenplum', 'GP_DB')
# gp_usr = myutil.get_conf( 'Greenplum', 'GP_USER')
# gp_pw = myutil.get_conf( 'Greenplum', 'GP_PASSWORD')
# db = mydb.Mydb(gp_host, gp_port, gp_db, gp_usr, gp_pw)


class dbsession_wrapper(object):
    def __init__(self, wrapped):
        self.wrapped = wrapped
        functools.update_wrapper(self, wrapped)

    def __call__(self, *args, **kwargs):
        arg_session = 'session'

        func_params = self.wrapped.__code__.co_varnames
        session_in_args = arg_session in func_params and \
            func_params.index(arg_session) < len(args)
        session_in_kwargs = arg_session in kwargs

        if session_in_kwargs or session_in_args:
            return self.wrapped(*args, **kwargs)
        else:
            with db.create_session() as session:
                kwargs[arg_session] = session
                return self.wrapped(*args, **kwargs)

        return self.wrapped(*args, **kwargs)

Base = declarative_base()
# class RuleOperator(Base):
class RuleOperator(Base):
    __tablename__ = "dl_rule_operator"
    __table_args__ = {'schema': 'edw'}
    id = Column(Integer, primary_key = True,autoincrement=True)
    name = Column(String(255), nullable=False)
    order = Column(Integer, nullable=False)
    pre_factor = Column(Integer, nullable=False)
    rule = Column(Text, nullable=False)
    factor = Column(Text, nullable=False)
    action = Column(Text, nullable=False)
    action_type = Column(String(255), nullable=False)
    error_level = Column(String(255), nullable=True)
    state = Column(String(5), nullable=True)
    comment = Column(Text, nullable=True)

    def __init__(self, 
                name, 
                order, 
                pre_factor, 
                rule, 
                factor, 
                action, 
                action_type, 
                error_level = 'error', 
                state = None, 
                comment =None ):
        self.name =  name
        self.order = order
        self.pre_factor =  pre_factor
        self.rule = rule
        self.factor = factor
        self.action = action
        self.action_type = action_type
        self.error_level = error_level
        self.state = state
        self.comment = comment
        self._result = None
    
    def __str__(self):
         return '{\n name:"%s",\n order:"%d",\n pre_factor:"%d,\n rule:"%s",\n factor:"%s",\n action:"%s",\n action_type:"%s"\n}'%(self.name, self.order, self.pre_factor, self.rule, self.factor, self.action, self.action_type)
        
    def replace_factor_with_result(self, node):
        self.factor =  self.factor.replace(node.factor, node.result)

    @classmethod
    @dbsession_wrapper
    def select_rules_by_names(cls, names=None, session = None):
        if names is None:
            return session.query(RuleOperator).order_by(RuleOperator.name).all()
        else:
            return session.query(RuleOperator).filter_by(RuleOperator.name.in_(names.split(","))).order_by(RuleOperator.name).all()

    @classmethod
    def init_db(self):
        Base.metadata.create_all(db.create_engine())

    @classmethod
    @dbsession_wrapper
    def add_operator_rule(cls, operator, session = None):
        session.add(operator) 

    @property
    def result(self):
        return self._result

    @result.setter
    def result(self, rs):
        if rs is None:
            return
        if not isinstance(rs, str) or len(rs)> 20  or len(rs) == 0:
            print("RuleOperator's result must be string and 0<length<=20 or None 。 rs = %s"%(rs))
            raise ValueError("RuleOperator's result must be string and length<=20 or None")
        self._result = rs


class MyRete():
    rete_key_words = ('and','or', '==', '>','<','>=','<=')
    def __init__(self, name, rule_list):
        if (not isinstance(rule_list, List)):
            raise ValueError("Initiation error: rule_list is not List")

        self.rule_list = rule_list
        self.is_validate = False
        self.result = False
        self.name = None
        self._error_msg = []
        self._warning_msg = []

    @classmethod
    def validate_ruleset(cls,  rule_list):
        if (not isinstance(rule_list, List)):
            raise ValueError("Initiation error: rule_list is not List") 

        rule_list = sorted(rule_list, key=lambda x: (x.pre_factor+1)*100 + x.order, reverse=True)
        #规则校验规则
        # 1. the pre_factor must start_from 0
        # 2. pre_factor是连续的
        # 3. pre_factor<order
        # 4. 所有的order是连续的 
        pre_rule = None
        max_order = 1
        min_order = 1
        for rule in rule_list:
            max_order = max(max_order, rule.order)
            min_order = min(min_order, rule.order)
            if pre_rule is None:
                pre_rule = rule
                continue
            if rule.pre_factor not in (pre_rule.pre_factor - 1, pre_rule.pre_factor):
                raise ValueError("Rule's pre-factor must be serial numbers")
            if rule.pre_factor >= rule.order:
                raise ValueError("Rule's pre-factor must greater than rule's order")
            
            cls.validate_rule_str(rule.action, rule.action_type) 
            pre_rule = rule 
        else:
            if pre_rule is not None and pre_rule.pre_factor > 0:
                raise ValueError("Rule's pre-factor must start from 0")
            
            if min_order<1 or max_order != len(rule_list):
                raise ValueError("Rule's must start from 1 and be unique ")
        
        return rule_list
    
    @classmethod
    def validate_rule_str(cls, rule_str, rule_type):
        rule_str = rule_str.strip(" ").strip(';')
        if rule_type == SQL_TYPE:
            if ( ';' in rule_str 
                or rule_str.lower().find("select") != 0):
                raise ValueError("Cannot execute >1 SQL statement in 1 rule and must be select query")
        elif rule_type == EXP_TYPE:
            for fc in rule_str.split(" "):
                if keyword.iskeyword(fc) and fc not in MyRete.rete_key_words :
                   raise ValueError("Rule factor cannot contain Python keyword") 

                for kw in MyRete.rete_key_words:
                    if kw in fc:
                        fc = fc.replace(kw, ' ')

                for subfc in fc.split(" "):
                    if (len(subfc) > 32 
                        or  not re.match("^[0-9A-Za-z]*$", subfc) ):
                        raise ValueError("Rule factor is composited by character and alphanumeric characters.")
        return True

    @property
    def error_msg(self):
        return self._error_msg
    
    @property
    def warning_msg(self):
        return self._warning_msg

    #执行单个规则序列
    def execute_rete_rules(self):
        if ( not self.is_validate ):
            try:
                self.rule_list = self.validate_ruleset(self.rule_list)
                self.is_validate = True
            except ValueError as e:
                self.add_rete_error(ERROR_LEVEL_FATAL, str(e))
                return 

        for node in self.rule_list:
            # print("execute rule, pre_factor = %d, order=%d, action = %s"%( node.pre_factor, node.order, node.action))
            try:
                rs = ExecutorManager.execute(node)
                node.result = rs
                if rs =='False' and node.state not in (OPERATOR_STATE_SKIP, OPERATOR_STATE_CHILD_FALSE):
                    self.add_rete_error(node.error_level, node.comment)
                    node.state = OPERATOR_STATE_FALSE

                self._replace_parent_factor(node)

                #replace parent's factor
            except Exception as e:
                print(e)
                # print("Failed to excute the node: order = %d"%(node.order))
                self.add_rete_error(node.error_level, str(e))
                break
        else:
            self.result = self.rule_list[-1].result
        return self.result
    
    #收集错误信息
    def add_rete_error(self, info_level, msg):
        if info_level.upper() == ERROR_LEVEL_FATAL:
            self._error_msg.append(msg)
        elif info_level.upper() == ERROR_LEVEL_WARNING:
            self._warning_msg.append(msg)

    # 将子节点的结果，更新到对应的算子
    def _replace_parent_factor(self, childnode ):
        if childnode is None or childnode.result is None:
            return
        
        for node in self.rule_list:
            if node.order == childnode.pre_factor:
                node.action = node.action.replace(childnode.factor,  childnode.result)
                if childnode.result == 'False':
                    node.state = OPERATOR_STATE_FALSE
                break

class ExecutorManager():
    def __init__():
        ...

    @classmethod
    def execute(cls, node:RuleOperator):
        if node.action_type.upper() == SQL_TYPE:
            return cls._execute_sql(node)
        elif node.action_type.upper() == EXP_TYPE:
            return cls._execute_exp(node)

    @classmethod
    def _execute_sql(self, node:RuleOperator):
        engine = db.create_engine()
        conn = db.create_conn(engine)
        r = conn.execute(node.action).fetchone()
        db.close_conn(conn)
        
        return str(r[0]) if r is not None else None
    
    @classmethod
    def _execute_exp(self, node:RuleOperator):
        env = {}
        env["locals"]   = None
        env["globals"]  = None
        env["__name__"] = None
        env["__file__"] = None
        env["__builtins__"] = None
        # print(eval(node.action))
        return str(eval(node.action))


def execute_rete(name, rule_list):
    myrete = MyRete(name, rule_list)
    myrete.execute_rete_rules()
    return myrete

def excute_rete_from_db( rule_names = None ):
    rules = RuleOperator.select_rules_by_names(rule_names)
    error_msg = defaultdict(List)
    warning_msg = defaultdict(List)

    pre_rule = None
    rule_list = []
    for ru in rules:
        if pre_rule is None:
            pre_rule = ru
            rule_list.append(ru)
            continue

        if pre_rule.name != ru.name:
            myrete =  execute_rete(pre_rule.name, rule_list)
            if len(myrete.error_msg) > 0:
               error_msg[pre_rule.name] = myrete.error_msg

            if len(myrete.warning_msg) > 0:
                warning_msg[pre_rule.name] = myrete.warning_msg
            rule_list = [ru]
        else:
            rule_list.append(ru)    
        
        pre_rule = ru
    else:
        if pre_rule:
            myrete = execute_rete(pre_rule.name, rule_list)
            if len(myrete.error_msg) > 0:
                error_msg[pre_rule.name] = myrete.error_msg

            if len(myrete.warning_msg) > 0:
                warning_msg[pre_rule.name] = myrete.warning_ms
    
    return (error_msg, warning_msg)

from datetime import datetime
@dbsession_wrapper
def collect_tables_input_record(session =None):
    collect_sql_dict = myutil.get_sql_yml_fd('collect_table_daily_input') 
    stg_collect_sql = collect_sql_dict["collect_stg_count"]
    edw_collect_sql = collect_sql_dict["collect_edw_count"]
    stg_count_list = session.execute(stg_collect_sql).fetchall()
    edw_count_list = session.execute(edw_collect_sql).fetchall()
    stg_count_dict = defaultdict(int)
    for name, count in stg_count_list:
        stg_count_dict[name] = count

    table_report_html_str = datetime.strftime(datetime.now(),'%Y-%m-%d') + " CDP收数表"
    table_report_html_str += '''
    <table border="1">
    <tr>
        <th>name</th>
        <th>stg_count</th>
        <th>edw_count</th>
        <th>frequency</th>
    </tr>
    '''
    for rk, name, record_count, frq in edw_count_list:
        row_str = "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format(name, stg_count_dict[name], record_count, frq)
        table_report_html_str += row_str
    table_report_html_str += "</table>"
    return table_report_html_str
    

def generate_data_scan_report(err, warning, scan_table = True):
    email_body = '<html> <body>'
    if scan_table:
        email_body += collect_tables_input_record()
    
    email_body += '='*60 + "\n"
    email_body += "<h3>自定义数据检验规则结果<h3> \n"
    email_body += "<h4>Error message:<h4> \n<div><ol>\n"
    for k in err.keys():
        email_body += "<li><b>{}</b></li>\n".format(k)
        email_body += "<ul> \n"
        for msg in err[k]:
            # print(msg)
            email_body += "<li>{}</li>\n".format(msg)
        email_body += "</ul></div>\n"
    email_body += "</ol></div>\n"
    
    email_body += '<h4>Warning message:<h4>\n'
    email_body += "<div><ol>\n"
    for k in warning.keys():
        email_body += "<li><b>{}</b></li>\n".format(k)
        email_body += "<ul> \n"
        for msg in warning[k]:
            email_body += "<li>{}</li>\n".format(msg)
        email_body += "</ul></div>\n"
    email_body += "</ol></div>\n"
    email_body += "</body></html>"
    return email_body