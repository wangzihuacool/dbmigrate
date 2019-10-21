# -*- code: utf-8 -*-
'''
  连接mysql操作数据库
'''
import pymysql
import random
import time
import traceback
import warnings
import pysnooper
from comm_decorator import performance


warnings.filterwarnings("ignore")


# mysql数据库操作类
class MysqlOperate(object):
    def __init__(self, **db_info):
        self.host = db_info.get('host')
        self.port = db_info.get('port')
        self.db = db_info.get('db')
        self.user = db_info.get('user')
        self.password = db_info.get('password')
        self.charset = 'utf8mb4' if not db_info.get('charset') else db_info.get('charset')
        self.connection = pymysql.connect(host = self.host,
                                          port = self.port,
                                          db = self.db,
                                          user = self.user,
                                          password = self.password,
                                          charset = self.charset)

    #mysql查询语句
    #@performance
    def mysql_select(self, sql):
        with self.connection.cursor() as conn_cursor:
            conn_cursor.arraysize = 10000  # 设置一次批量获取的行数，对fetchall无效
            conn_cursor.execute(sql)
            results = conn_cursor.fetchall()
            return results

    #mysql批量获取
    #@performance
    def mysql_select_incr(self, sql, arraysize=10000):
        with self.connection.cursor() as conn_cursor:
            conn_cursor.arraysize = arraysize  # 设置一次批量获取的行数，对fetchall无效
            conn_cursor.execute(sql)
            while True:
                results = conn_cursor.fetchmany()
                yield results
                if not results:
                    break


    #带参数的mysql查询语句
    #@pysnooper.snoop()
    def mysql_select_param(self, sql, args):
        with self.connection.cursor() as conn_cursor:
            conn_cursor.arraysize = 10000
            conn_cursor.execute(sql, args)
            results = conn_cursor.fetchall()
            return results

    # mysql的DML语句
    def mysql_execute(self, sql, *args):
        with self.connection.cursor() as conn_cursor:
            try:
                conn_cursor.execute('start transaction')
                conn_cursor.execute(sql, *args)
                self.connection.commit()
                numrows = conn_cursor.rowcount
            except Exception as e:
                traceback.print_exc()
                self.connection.rollback()
                numrows = 0
            finally:
                return numrows

    # mysql的DCL语句或不需要事务的DML语句
    def mysql_execute_no_trans(self, sql, *args):
        with self.connection.cursor() as conn_cursor:
            try:
                conn_cursor.execute(sql, *args)
                numrows = conn_cursor.rowcount
            except Exception as e:
                traceback.print_exc()
                self.connection.rollback()
                numrows = 0
            finally:
                return numrows

    #mysql的批量插入语句
    #@performance
    def mysql_executemany(self, sql, data):
        with self.connection.cursor() as conn_cursor:
            try:
                conn_cursor.execute('start transaction')
                conn_cursor.executemany(sql, data)
                self.connection.commit()
                numrows = conn_cursor.rowcount
            except Exception as e:
                traceback.print_exc()
                self.connection.rollback()
                numrows = 0
            finally:
                return numrows

    #转换行记录返回dict格式
    def Rows_as_Dicts(self, conn_cursor):
        col_names = [i[0].lower() for i in conn_cursor.description]
        return [dict(zip(col_names, row)) for row in conn_cursor]

    #返回dict格式的查询结果集
    def execute_dict(self, sql):
        with self.connection.cursor() as conn_cursor:
            conn_cursor.execute(sql)
            results = self.Rows_as_Dicts(conn_cursor)
        return results

    #关闭mysql连接
    def close(self):
        self.connection.close()




# main
if __name__ == "__main__":
    mysql_info = {'host': '172.20.227.35', 'port': 3306, 'db': 'information_schema', 'user': 'root', 'password': 'frPxYRVX67dE', 'charset': 'utf8'}
    MysqlDb = MysqlOperate(**mysql_info)
    res = MysqlDb.mysql_select('select * from sbench.sbtest1 order by id limit 2')
    print(res)
    print(len(res))
    sbtest_data = ((10000001, 501462, '68487932199-96439406143-93774651418-41631865787-96406072701-20604855487-25459966574-28203206787-41238978918-19503783441', '22195207048-70116052123-74140395089-76317954521-98694025897'), (10000002, 502480, '13241531885-45658403807-79170748828-69419634012-13605813761-77983377181-01582588137-21344716829-87370944992-02457486289', '28733802923-10548894641-11867531929-71265603657-36546888392'))
    #insert_rows = MysqlDb.mysql_executemany('insert into sbtest1(id, k, c, pad) values (%s, %s, %s, %s)', sbtest_data)
    #print(insert_rows)