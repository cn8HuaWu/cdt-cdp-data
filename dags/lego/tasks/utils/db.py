from sqlalchemy import Column, Integer, String,  Boolean
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging
from sqlalchemy.exc import InternalError

class Mydb(object):
    def __init__(self, h, p, db, usr, pw):
        super().__init__()
        self.host = h 
        self.port = p
        self.db = db
        self.user = usr
        self.password = pw

    def create_engine(self, debug_flag = False):
        conn_dict = {
            "user": self.user,
            "password": self.password,
            'host': self.host,
            "port": self.port,
            "db": self.db 
        }
        engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{db}'.format_map(conn_dict), echo=debug_flag)
        return engine

    def create_conn(self, engine):
        conn = engine.connect()
        # conn = conn.execution_options(isolation_level="SERIALIZABLE")
        return conn

    def execute(self, sql_str, conn, retry=3):
        max_retry = 1 if retry<=0 else retry
        while( max_retry > 0):
            try:
                trans = conn.begin()
                r = conn.execute(sql_str) 
                trans.commit()
                break
            except InternalError:
                trans.rollback()
                max_retry -= 1
                if (max_retry <= 0):
                    raise
            except:
                trans.rollback()
                raise
        return r
    
    def close_conn(self, conn):
        try:
            conn.close()
        except:
            logging.warning("Failed to close the db connection")