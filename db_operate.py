#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import cx_Oracle


#数据库操作类
class DbOperate(object):

    #定义连接池对象,__ConnPool为类变量，整个类里面可以共用，调用时需加上类名
    __ConnPool = None


    """
    oracle数据库连接模块
    1) connect的连接方式：默认的连接方式，保持session_pool为None；
                    oracle连接支持tnsnames和sid以及service_name的方式，优先通过TNSNAMES其次service_name方式
    2) SessionPool的连接方式：当session_pool不为None时启用SessionPool连接池，连接池只通过service_name方式连接
    """
    def __init__(self, host, port, username, password, db=None, sid=None,
                 service_name=None, mode=None, session_pool=None, min=None,
                 max=None, increment=None, timeout=None, charset="utf8"):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.sid = sid
        self.service_name = service_name
        self.db = db
        self.mode = mode
        self.session_pool = session_pool
        self.min = min
        self.max = max
        self.increment = increment
        self.timeout = timeout

        #判断是否启用连接池
        if self.session_pool:
            self.conn = DbOperate.get_session_pool(self.username, self.password, self.host, self.port, self.service_name,
                                                   mode=self.mode, min=self.min, max=self.max, increment=self.increment,
                                                   timeout=self.timeout)
        #如果不启用连接池，还是沿用之前的连接方式（推荐service_name）
        else:
            if self.mode is None:
                if self.db:
                    self.conn = cx_Oracle.connect(self.username, self.password, self.db)
                else:
                    if self.service_name:
                        self.handle = "%s:%s/%s" % (self.host, str(self.port), self.service_name)
                        self.conn = cx_Oracle.connect(self.username, self.password, self.handle)
                    elif self.sid:
                        self.dsn = cx_Oracle.makedsn(self.host, str(self.port), self.sid)
                        self.conn = cx_Oracle.connect(self.username, self.password, self.dsn)
            else:
                if self.db:
                    self.conn = cx_Oracle.connect(self.username, self.password, self.db, mode=cx_Oracle.SYSDBA)
                else:
                    if self.service_name:
                        self.handle = "%s:%s/%s" % (self.host, str(self.port), self.service_name)
                        self.conn = cx_Oracle.connect(self.username, self.password, self.handle, mode=cx_Oracle.SYSDBA)
                    elif self.sid:
                        self.dsn = cx_Oracle.makedsn(self.host, str(self.port), self.sid)
                        self.conn = cx_Oracle.connect(self.username, self.password, self.dsn, mode=cx_Oracle.SYSDBA)

        self.cursor = self.conn.cursor()


    # 数据库构造函数，返回连接池DbOperate.__ConnPool，从连接池中取出连接
    #静态方法无需实例化即可调用
    @staticmethod
    def get_session_pool(username, password, host, port, service_name, mode=None,
                         min=None, max=None, increment=None,timeout=None):
        #如果连接池没有创建，则创建连接池(注意：连接池方式不能使用sysdba连接)
        if DbOperate.__ConnPool is None:
            #print('Init SessionPool __ConnPool')
            handle = "%s:%s/%s" % (host, str(port), service_name)
            DbOperate.__ConnPool = cx_Oracle.SessionPool(username, password, handle, min=min, max=max,
                                                         increment=increment,timeout=timeout)
            #if mode is None:
            #    handle = "%s:%s/%s" % (host, str(port), service_name)
            #    DbOperate.__ConnPool = cx_Oracle.SessionPool(username, password, handle, min=min, max=max,
            #                                                 increment=increment,timeout=timeout)
            #else:
            #    handle = "%s:%s/%s" % (host, str(port), service_name)
            #    DbOperate.__ConnPool = cx_Oracle.SessionPool(username, password, handle, min=min, max=max,
            #                                                increment=increment,timeout=timeout, mode = cx_Oracle.SYSDBA)
        #如果连接池已创建，则可以直接从连接池获取连接
        else:
            print('SessionPool __ConnPool exists,Acquire connection from pool')
            #print(str(DbOperate.__ConnPool.timeout) + 's')

        #打印SessionPool中当前已分配的会话数和打开的会话数
        #print('Session acquired: ' + DbOperate.__ConnPool.busy)
        #print('Session opened: ' + DbOperate.__ConnPool.opened)
        #返回connection对象
        return DbOperate.__ConnPool.acquire()


    def get_db_cursor(self):
        return self.cursor

    def execute(self, sql):
        self.cursor.arraysize = 10000  # 设置一次批量获取的行数, 对fetchall无效
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        return results

    def insertbatch(self, sql, params, arraydmlrowcounts=True):
        self.conn.begin()
        self.cursor.executemany(sql, params)
        self.conn.commit()
        numrows = self.cursor.rowcount
        return numrows

    def executedml(self, sql):
        self.conn.begin()
        self.cursor.execute(sql)
        self.conn.commit()
        numrows = self.cursor.rowcount
        return numrows

    #转换行记录返回dict格式
    def Rows_as_Dicts(self, cursor):
        col_names = [i[0].lower() for i in cursor.description]
        return [dict(zip(col_names, row)) for row in cursor]

    #返回dict格式的结果集
    def execute_dict(self, sql):
        self.cursor.execute(sql)
        results = self.Rows_as_Dicts(self.cursor)
        return results

    #定义返回lob类型字段的fetchall方法
    def _fetchall(self):
        if any(x[1] == cx_Oracle.CLOB for x in self.cursor.description):
            return [tuple([(c.read() if type(c) == cx_Oracle.LOB else c) \
                           for c in r]) for r in self.cursor]
        else:
            return self.cursor.fetchall()

    #返回LOB字段的结果集
    def execute_lob(self, sql):
        self.cursor.execute(sql)
        results = self._fetchall()
        return results

    def close(self):
        self.cursor.close()
        self.conn.commit()
        #可以直接close连接
        self.conn.close()

        #也可以从DbOperate.__ConnPool中release连接
        #if self.session_pool:
        #    self.cursor.close()
        #    DbOperate.__ConnPool.release(self.conn)


class DbMonitor(object):
    """
    monitor数据库操作模块:查询，插入
    """

    #这里monitor库使用连接池的方式
    def __init__(self):
        db_monitor = DbOperate("ip","port","username", "password", service_name="service_name",
                               session_pool='Y', min=1, max=100, increment=10, timeout=10)
        self.db_monitor = db_monitor

    def get_db_cursor(self):
        return self.db_monitor.get_db_cursor()

    def execute(self, sql_select):
        results = self.db_monitor.execute(sql_select)
        return results

    def execute_dict(self, sql_select):
        results = self.db_monitor.execute_dict(sql_select)
        return results
    
    def execute_lob(self, sql_select):
        results = self.db_monitor.execute_lob(sql_select)
        return results

    def get_mon(self):
        sql_getsid = "select object_name from dba_objects where owner = 'EMLOG'and object_type = 'DATABASE LINK' order by object_name"
        dbs = self.db_monitor.execute(sql_getsid)
        return dbs

    def get_pass(self, name):
        sql_getpass = "select distinct(password) from dbuser where dblink_name = '" + name + "' and username = 'sys'"
        password = self.db_monitor.execute(sql_getpass)
        return password

    def insert_mon(self, sql_insert, params):
        insertrows = self.db_monitor.insertbatch(sql_insert, params)
        return insertrows

    def executedml(self, sql_dml):
        dmlrows = self.db_monitor.executedml(sql_dml)
        return dmlrows

    def get_db_ansi(self, db_id):
        # 从monitor数据库中根据db_id获取数据库连接串,公用
        SQL_METRIC_DBINFO = """
        select db_id as db_id,
               db_name as db_name,
               main_ip as client_ip,
               db_port1 as client_port,
               db_instance_name as service_name,
               db_user_name as username,
               db_user_passwd as password
            from vivo_dblist_ansi_v
            where env_mode = 'PROD'
            and UPPER(db_type_name) in ('ORACLE', 'ORACLE RAC')
            -- and upper(db_role) = 'PRIMARY'
            and db_user_name = 'sys'
            and db_id = {db_id}
            -- and db_name = 'db_name'
            order by db_id, db_instance_name
        """
        dbs = self.db_monitor.execute_dict(SQL_METRIC_DBINFO.format(db_id=db_id))
        db_info = dbs[0]
        return db_info

    def get_db_ansi_mysql(self, db_id):
        # 从monitor数据库中根据db_id获取mysql数据库连接串,公用
        SQL_MYSQL_DBINFO = """
                           select db_id as db_id,
                                  db_name as db_name,
                                  main_ip as client_ip,
                                  db_port1 as client_port,
                                  db_instance_name as service_name,
                                  db_user_name as username,
                                  db_user_passwd as password
                             from vivo_dblist_ansi_v
                            where env_mode = 'PROD'
                              and upper(db_type_name) = 'MYSQL'
                              and (db_user_name = 'vivo_db_monitor' or db_user_name = 'VIVO_DB_MONITOR')
                              and db_id = {db_id}
                           """
        dbs = self.db_monitor.execute_dict(SQL_MYSQL_DBINFO.format(db_id=db_id))
        db_info = dbs[0]
        return db_info

    def close(self):
        self.db_monitor.close()


if __name__ == "__main__":
    pass
