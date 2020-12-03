#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymssql
import traceback
from functools import reduce


# 数据库操作类
class MssqlOperate(object):
    # 检查目标库连接
    def __init__(self, db_info):
        self.conn = MssqlOperate.mssql_conn(db_info)

    # Mssql连接
    @staticmethod
    def mssql_conn(db_info, retries=5):
        i = 1
        while i <= retries:
            try:
                host = db_info.get('host')
                port = db_info.get('port')
                db = db_info.get('db')
                user = db_info.get('user')
                password = db_info.get('password')
                charset = 'utf8' if not db_info.get('charset') else db_info.get('charset')
                connection = pymssql.connect(server=host,
                                             port=port,
                                             database=db,
                                             user=user,
                                             password=password,
                                             charset=charset,
                                             timeout=0)
            except Exception as e:
                traceback.print_exc()
                connection = None
            else:
                break
            finally:
                i += 1
        else:
            print('[DBM Monitor]: can not connect to ' + db_info.get('host') + ':' + str(
                db_info.get('port')) + '/' + db_info.get('db'))
        return connection

    # 检查数据库连接
    def check_db_conn(self):
        return 1 if self.conn else 0

    # 返回连接的cursor对象
    def cursor(self):
        return self.conn.cursor() if self.conn else None

    # 执行SQL，获取全量结果
    def execute(self, sql):
        with self.conn.cursor() as conn_cursor:
            conn_cursor.execute(sql)
            results = conn_cursor.fetchall()
        return results

    # 全量获取，附带列信息
    def execute_description(self, sql):
        with self.conn.cursor() as conn_cursor:
            conn_cursor.execute(sql)
            results = conn_cursor.fetchall()
            desc = conn_cursor.description
        return desc, results

    # @performance
    # 执行SQL，增量获取，外层调用时需要循环调用
    def execute_incr(self, sql, arraysize=100000):
        with self.conn.cursor() as conn_cursor:
            conn_cursor.execute(sql)
            while True:
                results = conn_cursor.fetchmany(size=arraysize)
                yield results

    # 获取源表列名，用于源库和目标库的表结构不完全一致的情况 added by wl_lw at 20201202
    def mssql_columns(self, sql):
        with self.conn.cursor() as conn_cursor:
            fake_sql = 'select top 0 * from (' + sql + ') t'
            conn_cursor.execute(fake_sql)
            col_names = [i[0].lower() for i in conn_cursor.description]
        return col_names

    # 批量插入
    def insertbatch(self, sql, params):
        with self.conn.cursor() as conn_cursor:
            self.conn.begin()
            try:
                conn_cursor.executemany(sql, params)
                self.conn.commit()
            except Exception as e:
                traceback.print_exc()
                self.conn.rollback()
            numrows = conn_cursor.rowcount
        return numrows

    # 执行DML操作
    def executedml(self, sql):
        with self.conn.cursor() as conn_cursor:
            self.conn.begin()
            conn_cursor.execute(sql)
            self.conn.commit()
            numrows = conn_cursor.rowcount
        return numrows

    # 转换行记录返回dict格式
    def Rows_as_Dicts(self, cursor):
        col_names = [i[0].lower() for i in cursor.description]
        return [dict(zip(col_names, row)) for row in cursor]

    # 返回dict格式的结果集
    def execute_dict(self, sql):
        with self.conn.cursor() as conn_cursor:
            conn_cursor.execute(sql)
            results = self.Rows_as_Dicts(conn_cursor)
        return results

    # 执行DDL操作
    def execute_ddl(self, sql):
        self.conn.cursor().execute(sql)

    def close(self):
        self.conn.cursor().close()
        self.conn.commit()
        # 可以直接close连接
        self.conn.close()


# MAIN
if __name__ == '__main__':
    mssql_info = {'host': '172.20.xx.xx', 'port': 1433, 'db': 'xxx', 'user': 'xxx',
                  'password': 'xxx', 'db_type': 'mssql', 'charset': 'utf8'}
    mssql_operate = MssqlOperate(mssql_info)
    flag = mssql_operate.check_db_conn()
    print(flag)
    sql = '''select top 1 * from (Select top 100 * from mail_instance) t'''

    # 全量获取
    desc, res = mssql_operate.execute_description(sql)
    print(desc)
    # print(res)
    print(len(res))
    '''
    # 增量获取
    res1 = mssql_operate.execute_incr(sql, arraysize=10000)
    numrows_list = []
    while True:
        res_data = next(res1)
        if res_data:
            # print(res_data)
            print(len(res_data))
            numrows_list.append(len(res_data))
        else:
            break
    total_rows = reduce(lambda x, y: x + y, numrows_list) if numrows_list else 0
    print(total_rows)

    # Dict格式获取
    res2 = mssql_operate.execute_dict(sql)
    print(res2)
    mssql_operate.close()
    '''