# -*- code: utf-8 -*-

'''
mysql迁移源库和目标库类
'''
import traceback
from operator import itemgetter
from operator import ne, eq
from functools import reduce, partial
from multiprocessing import cpu_count, Process, Pool
import threading
import os, sys
import math
import copy
from db_operate import DbOperate
from comm_decorator import performance, MyThread


# 源库为oracle时获取数据
class OracleSource(object):
    pass
    # to_do

    '''
    (这部分参考mysql，先判断源表是否存在）
    #检查配置文件中的表在源库是否存在，未配置则全部表同步
    def source_table_check(self, *source_tables):
        res_tables = self.MysqlDb.mysql_select('show full tables from `%s` where table_type != "VIEW"' % self.from_db)
        all_table_list = [table[0] for table in res_tables]
        if source_tables:
            if set(all_table_list) >= set(source_tables):
                from_tables = source_tables
                migrate_granularity = 'table'
            else:
                not_exists_tables = set(source_tables) - set(all_table_list)
                print('[DBM] Error: 源数据库[' + self.from_db + ']中不存在表:' + str(not_exists_tables) + '.请确认!')
                sys.exit(1)
        else:
            from_tables = all_table_list
            migrate_granularity = 'db'
        return from_tables, migrate_granularity
    '''

# 目标库为oracle时装载数据
class OracleTarget(object):
    # 检查目标库连接
    def __init__(self, **target_db_info):
        self.hostname = target_db_info.get('host')
        self.port = target_db_info.get('port')
        self.service_name = target_db_info.get('db')
        self.username = target_db_info.get('user')
        self.password = target_db_info.get('password')
        self.charset = target_db_info.get('charset')
        try:
            self.OracleTargetConn = DbOperate(self.hostname, self.port, self.username, self.password,
                                              service_name=self.service_name, session_pool='Y', min=1, max=100,
                                              increment=4, charset=self.charset)
        except Exception as e:
            print('DBM Error: can not connect to target db: ' + target_db_info.get('host') + ':' +
                  str(target_db_info.get('port')) + '/' + target_db_info.get('db'))
            traceback.print_exc()
            sys.exit(1)

    # Oracle目标库元数据创建
    def oracle_metadata(self):
        pass
        # to_do

    # Oracle检查目标库已存在的表
    def oracle_target_exist_tables(self):
        res_tables = self.OracleTargetConn.execute('select lower(table_name) from user_tables')
        all_table_list = [table[0] for table in res_tables]
        return all_table_list

    # 传输数据到目标表
    def insert_target_data(self, to_table, data):
        if data:
            str_list = [(':' + str(i)) for i in range(len(data[0]))]
            value_str = ','.join(str_list)
            insert_sql = 'insert into ' + to_table + ' values (' + value_str + ')'
            # cx-Oracle调用executemany插入时需要数据集为元祖组成的list
            data = list(data) if not isinstance(data, list) else data
            data_rows = self.OracleTargetConn.insertbatch(insert_sql, data)
            return data_rows
        else:
            return 0

    # 目标库执行SQL
    def oracle_execute_dml(self, sql):
        affect_rows = self.OracleTargetConn.executedml(sql)
        return affect_rows

    def mysql_target_close(self):
        self.OracleTargetConn.close()


# 源库是oracle时的并行/串行数据处理
class OracleDataMigrate(object):
    def __init__(self, source_db_info, target_db_info):
        from mysql_migrate import MysqlSource
        self.source_db_info = source_db_info
        self.target_db_info = target_db_info
        self.mysql_source = MysqlSource(**source_db_info)
        self.oracle_target = OracleTarget(**target_db_info)

    def oracle_serial_migrate(self):
        pass
        # to_do

