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
import os
import math
import copy
from mysql_operate import MysqlOperate
from comm_decorator import performance, MyThread
#import warnings
#from itertools import groupby



#源库为mysql时获取数据
class MysqlSource(object):
    # 检查源数据库连接
    def __init__(self, **source_db_info):
        try:
            self.MysqlDb = MysqlOperate(**source_db_info)
            self.from_db = source_db_info.get('db')
            self.MysqlDb.mysql_execute('use %s' %self.from_db)
        except Exception as e:
            print('[DBM] Error: can not connect to source db: ' + source_db_info.get('host') + ':' +
                  str(source_db_info.get('port')) + '/' + source_db_info.get('db'))
            traceback.print_exc()
            exit(1)

    #检查源库是否存在
    def source_db_check(self):
        res_db = self.MysqlDb.mysql_select('show databases')
        if self.from_db in [db[0] for db in res_db]:
            pass
        else:
            print('[DBM] Error: Source db [' + self.from_db + '] not found.请确认!')
            exit(1)

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
                exit(1)
        else:
            from_tables = all_table_list
            migrate_granularity = 'db'
        return from_tables, migrate_granularity

    #获取源表元数据
    def mysql_source_table(self, from_table):
        res_tablestatus = self.MysqlDb.execute_dict('show table status from `%s` like "%s"' % (self.from_db, from_table))
        res_createtable = self.MysqlDb.execute_dict('show create table `%s`.`%s`' % (self.from_db, from_table))
        res_columns = self.MysqlDb.execute_dict('show full columns from `%s`.`%s`' % (self.from_db, from_table))
        res_triggers = self.MysqlDb.execute_dict('select trigger_name, action_order, event_object_table, '
                                                 'event_manipulation, definer, action_statement, action_timing '
                                                 'from information_schema.triggers '
                                                 'where binary event_object_schema="%s" and binary event_object_table="%s" '
                                                 'order by event_object_table' % (self.from_db, from_table))
        return res_tablestatus, res_createtable, res_columns, res_triggers

    #获取源表索引(除去主键,主键跟表元数据一起)
    def mysql_source_index(self, from_table):
        res_indexes = self.MysqlDb.execute_dict('show index from `%s`.`%s`' % (self.from_db, from_table))
        # 对mysql返回的索引列表排序
        res_indexes.sort(key=itemgetter('key_name', 'seq_in_index'))
        # 获取索引（唯一）和[多]列的对应关系，组成新的dict
        # dict.setdefault(key,default)：当key存在就返回key对应的value，key不存在时就设置为default,有点类似get()
        index_column_map = {}
        for row_index in res_indexes:
            index_column_map.setdefault(row_index.get('key_name'), []).append('`' + row_index.get('column_name') +
                                                                              '`' + ('(' + str(row_index.get('sub_part')) +
                                                                                     ')' if row_index.get('sub_part')
                                                                                     else '') +
                                                                              ((' desc') if row_index.get('collation') == 'D'
                                                                               else ''))
        # 对比新dict[index_column_def] 和 原[res_indexes]，获取索引详细信息(除去主键),组成index_column_info
        c = []
        for key, value in index_column_map.items():
            temp = {}
            for row_index in res_indexes:
                if key == row_index.get('key_name') and key != 'primary' and key != 'PRIMARY':
                    temp.setdefault('table', row_index.get('table'))
                    temp.setdefault('key_name', key)
                    temp.setdefault('non_unique', row_index.get('non_unique'))
                    temp.setdefault('column_name', ", ".join(value))
                    c.append(temp)
        # print(c)
        # 去重
        index_column_info = []
        [index_column_info.append(i) for i in c if i not in index_column_info]
        return index_column_info

    #全量获取源表数据
    def mysql_source_data_incr(self, sql, arraysize=10000):
        res_data_incr = self.MysqlDb.mysql_select_incr(sql, arraysize=arraysize)
        return res_data_incr

    #增量获取源表数据
    def mysql_source_data(self, sql):
        res_data = self.MysqlDb.mysql_select(sql)
        return res_data

    #获取源表涉及的外键
    def mysql_source_fk(self, from_tables):
        res_fk = self.MysqlDb.execute_dict('select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE where TABLE_SCHEMA="%s" '
                                           'and constraint_name not in ("PRIMARY", "primary")' % self.from_db)
        res_fk.sort(key=itemgetter('constraint_name', 'ordinal_position', 'position_in_unique_constraint'))
        #只获取需要同步的表涉及的外键
        filtered_fk = [fk_record for fk_record in res_fk if fk_record.get('table_name') in from_tables
                       and fk_record.get('referenced_table_name') in from_tables]
        #过滤掉外键为多列组合的情况
        comp_fk = [new_fk_record for new_fk_record in filtered_fk if new_fk_record.get('position_in_unique_constraint') == 1]
        final_fk = []
        for uniq_fk_record in comp_fk:
            for org_fk_record in filtered_fk:
                if uniq_fk_record['constraint_name'] == org_fk_record['constraint_name'] and ne(uniq_fk_record, org_fk_record):
                    uniq_fk_record['column_name'] = uniq_fk_record['column_name'] + ', ' + org_fk_record['column_name']
                    uniq_fk_record['referenced_column_name'] = uniq_fk_record['referenced_column_name'] + ', ' + \
                                                               org_fk_record['referenced_column_name']
            final_fk.append(uniq_fk_record)
        return final_fk

    #获取源库的所有视图,存储过程，函数和routines
    def mysql_source_pkg(self):
        res_views = self.MysqlDb.mysql_select('select table_name, check_option, is_updatable, security_type, '
                                              'definer from information_schema.views where table_schema="%s" '
                                              'order by table_name asc' % self.from_db)
        from_views = [view[0] for view in res_views]
        #获取源库的所有存储过程和函数和routines
        res_procedures = self.MysqlDb.mysql_select('show procedure status where db = "%s"' % self.from_db)
        from_procedures = [procedure[0] for procedure in res_procedures]
        res_functions = self.MysqlDb.mysql_select('show function status where db = "%s"' % self.from_db)
        from_functions = [function[0] for function in res_functions]
        res_routines = self.MysqlDb.mysql_select('select * from information_schema.routines where routine_schema="%s" '
                                                 'order by routine_name' % self.from_db)
        from_routines = [routine[0] for routine in res_routines]
        res_events = self.MysqlDb.mysql_select('select event_catalog, event_schema, event_name, definer, time_zone, '
                                               'event_definition, event_body, event_type, sql_mode, status, execute_at, '
                                               'interval_value, interval_field, starts, ends, on_completion, created, '
                                               'last_altered, last_executed, originator, character_set_client, '
                                               'collation_connection, database_collation, event_comment '
                                               'from information_schema.events '
                                               'where event_schema="%s" '
                                               'order by event_name asc' % self.from_db)
        from_events = [event[0] for event in res_events]
        return from_views, from_procedures, from_functions, from_routines, from_events


    def mysql_source_close(self):
        self.MysqlDb.close()



#目标为mysql时写入数据
class MysqlTarget(object):
    #检查目标库连接
    def __init__(self, **target_db_info):
        new_db_info = copy.deepcopy(target_db_info)
        new_db_info['db'] = None
        try:
            self.MysqlTarget = MysqlOperate(**new_db_info)
            self.to_db = target_db_info.get('db')
            self.MysqlTarget.mysql_execute('use %s' % self.to_db)
        except Exception as e:
            print('DBM Error: can not connect to target db: ' + target_db_info.get('host') + ':' +
                  str(target_db_info.get('port')) + '/' + target_db_info.get('db'))
            traceback.print_exc()
            exit(1)

    #创建目标数据库
    def mysql_target_createdb(self, migrate_granularity):
        res_db = self.MysqlTarget.mysql_select('show databases')
        if self.to_db in [db[0] for db in res_db] and migrate_granularity == 'db':
            self.MysqlTarget.mysql_execute('drop database if exists %s' % self.to_db)
            self.MysqlTarget.mysql_execute('create database if not exists %s' % self.to_db)
        elif self.to_db in [db[0] for db in res_db] and migrate_granularity == 'table':
            pass
        else:
            self.MysqlTarget.mysql_execute('create database if not exists %s' % self.to_db)

    #检查目标库已存在的表
    def mysql_target_exist_tables(self):
        res_tables = self.MysqlTarget.mysql_select('show full tables from `%s` where table_type != "VIEW"' % self.to_db)
        all_table_list = [table[0] for table in res_tables]
        return all_table_list

    #目标库创建表
    def mysql_target_table(self, to_table, table_exists_action, res_columns=None, res_tablestatus=None):
        self.MysqlTarget.mysql_execute('use %s' % self.to_db)
        #处理列信息
        all_columns_defination = ''
        for res_row in res_columns:
            field_defination = '`' + res_row.get('field') + '`'
            type_defination = res_row.get('type')
            charset_defination = ('character set ' + res_row.get('collation').split('_')[0]) if res_row.get('collation') else ''
            collation_defination = ('collate ' + res_row.get('collation')) if res_row.get('collation') else ''
            null_defination = 'not null' if res_row.get('null') == 'NO' else ''
            default_defination = ('default ' + res_row.get('default')) if res_row.get('default') else ''
            extra_defination = res_row.get('extra') if res_row.get('extra') else ''
            comment_defination = ('comment "' + res_row.get('comment') + '"') if res_row.get('comment') else ''
            column_defination = field_defination + ' ' + type_defination + ' ' + charset_defination + ' ' + \
                                collation_defination + ' ' + null_defination + ' ' + default_defination + ' ' + \
                                extra_defination + ' ' + comment_defination + ','
            all_columns_defination = all_columns_defination + column_defination
        all_columns_defination_1 = all_columns_defination.strip(",")

        #处理主键信息
        pri_key_col = [('`' + row.get('field') + '`') for row in res_columns if row.get('key') == 'PRI' or row.get('key') == 'pri']
        pri_key_defination = (' primary key (' + ", ".join(pri_key_col) + ')') if pri_key_col else ''

        #处理表默认属性
        engine_defination = 'engine=' + res_tablestatus[0].get('engine', 'InnoDB')
        default_charset_defination = ('default charset=' + res_tablestatus[0].get('collation').split('_')[0]) if res_tablestatus[0].get('collation') else ''
        default_comment_defination = ('comment="' + res_tablestatus[0].get('comment') + '"') if res_tablestatus[0].get('comment') else ''
        all_default_defination = engine_defination + ' ' + default_charset_defination + ' ' + default_comment_defination

        #创建目标表
        if pri_key_defination:
            sql_table_defination = 'create table `' + self.to_db + '`.`' + to_table + '` (' + all_columns_defination + pri_key_defination + ')' + all_default_defination
        else:
            sql_table_defination = 'create table `' + self.to_db + '`.`' + to_table + '` (' + all_columns_defination_1 + ')' + all_default_defination
        #print(sql_table_defination)
        print('[DBM] Create table `' + to_table + '`')
        table_rows = self.MysqlTarget.mysql_execute(sql_table_defination)

    #创建索引
    #@performance
    def msyql_target_index(self, to_table, index_column_info):
        print('[DBM] Create index on table `' + to_table + '`')
        all_indexes_defination = []
        for j in index_column_info:
            if j.get('non_unique') == 0:
                sql_index = 'alter table `' + self.to_db + '`.`' + j.get('table') + '` add unique ' + j.get('key_name') + '(' + j.get('column_name') + ')'
            else:
                sql_index = 'alter table `' + self.to_db + '`.`' + j.get('table') + '` add index ' + j.get('key_name') + '(' + j.get('column_name') + ')'
            all_indexes_defination.append(sql_index)
            index_rows = self.MysqlTarget.mysql_execute(sql_index)
        #print(all_indexes_defination)

    #创建外键
    def mysql_target_fk(self, final_fk):
        for final_fk_record in final_fk:
            sql_fk = 'alter table `' + final_fk_record.get('table_schema') + '`.`' + final_fk_record.get('table_name') + \
                     '` add constraint `' + final_fk_record.get('constraint_name') + '` foreign key (' + \
                     final_fk_record.get('column_name') + ') references `' + \
                     final_fk_record.get('referenced_table_schema') + '`.`' + final_fk_record.get('referenced_table_name') + \
                     '`(' + final_fk_record.get('referenced_column_name') + '`)'
            fk_rows = self.MysqlTarget.mysql_execute(sql_fk)
        #print(final_fk)

    #传输数据到目标表
    def mysql_target_data(self, to_table, data):
        if data:
            str_list = ['%s' for i in range(len(data[0]))]
            value_str = ','.join(str_list)
            insert_sql = 'insert into `' + self.to_db + '`.`' + to_table + '` values (' + value_str + ')'
            data_rows = self.MysqlTarget.mysql_executemany(insert_sql, data)
            return data_rows
        else:
            return 0

    #目标库执行sql
    def mysql_target_execute(self, sql):
        affect_rows = self.MysqlTarget.mysql_execute(sql)
        return affect_rows

    def mysql_target_close(self):
        self.MysqlTarget.close()




#源库和目标库都是mysql时的数据同步
#class MysqlSourceTarget(object):
#    def __init__(self, source_db_info, target_db_info):
#        self.source_db_info = source_db_info
#        self.target_db_info = target_db_info
#        self.mysql_source = MysqlSource(**source_db_info)
#        self.mysql_target = MysqlTarget(**target_db_info)
#
#    #mysql源库查询和目标库插入
#    def mysql_select_insert(self, sql_info):
#        to_table = sql_info.get('table_name')
#        sql_select = sql_info.get('sql_statement')
#        sql_data = self.mysql_source.mysql_source_data_incr(sql_select)
#        insert_rows = self.mysql_target.mysql_target_data(to_table, sql_data)
#        return insert_rows



#mysql源库查询和目标库插入(for 并行)
def mysql_select_insert(sql_info, source_db_info, target_db_info):
    mysql_source = MysqlSource(**source_db_info)
    mysql_target = MysqlTarget(**target_db_info)
    #优化同步逻辑，每进程内循环多次同步数据，每次取batch_rows行记录 --modified at 20190902
    from_db = sql_info.get('from_db')
    from_table = sql_info.get('from_table')
    to_table = sql_info.get('to_table')
    parallel_key = sql_info.get('parallel_key')
    from_rowid = sql_info.get('from_rowid')
    to_rowid = sql_info.get('to_rowid')
    max_key = sql_info.get('max_key')
    #每次同步10w行记录
    batch_rows = 100000
    batch_num = math.ceil((to_rowid - from_rowid)/batch_rows)
    insert_rows_list = []
    for b in range(batch_num):
        batch_from = from_rowid + (b * batch_rows)
        batch_to = to_rowid if (b == batch_num -1) else (from_rowid + ((b + 1) * batch_rows))
        if batch_to >= max_key:
            sql_select = 'select /*!40001 SQL_NO_CACHE */ * from `' + from_db + '`.`' + from_table + '` where `' + \
                         parallel_key + '` >= ' + str(batch_from) + ' and `' + parallel_key + '` <= ' + str(max_key)
        else:
            sql_select = 'select /*!40001 SQL_NO_CACHE */ * from `' + from_db + '`.`' + from_table + '` where `' + parallel_key + '` >= ' + str(batch_from) + ' and `' + parallel_key + '` < ' + str(batch_to)
        print(sql_select)
        sql_data = mysql_source.mysql_source_data(sql_select)
        insert_rows = mysql_target.mysql_target_data(to_table, sql_data)
        insert_rows_list.append(insert_rows)
    total_rows = reduce(lambda x, y: x + y, insert_rows_list)
    return total_rows


#mysql并行/串行处理
class MysqlDataMigrate(object):
    def __init__(self, source_db_info, target_db_info):
        self.source_db_info = source_db_info
        self.target_db_info = target_db_info
        self.mysql_source = MysqlSource(**source_db_info)
        self.mysql_target = MysqlTarget(**target_db_info)

    #mysql数据同步串行处理方法
    def mysql_serial_migrate(self, from_table, to_table):
        from_db = self.source_db_info.get('db')
        to_db = self.target_db_info.get('db')
        sql_select = 'select /*!40001 SQL_NO_CACHE */ * from `' + from_db + '`.`' + from_table + '`'
        sql_info = {'table_name': to_table, 'sql_statement': sql_select}
        print('[DBM] Inserting data into table `' + to_table + '`')
        arraysize = 10000
        res_data_incr = self.mysql_source.mysql_source_data_incr(sql_select, arraysize=arraysize)
        insert_rows_list = []
        while True:
            data_incr = next(res_data_incr)
            if data_incr:
                insert_rows = self.mysql_target.mysql_target_data(to_table, data_incr)
                insert_rows_list.append(insert_rows)
            else:
                break
        total_rows = reduce(lambda x, y: x + y, insert_rows_list) if insert_rows_list else 0
        return total_rows

    #mysql数据同步并行处理方法
    def mysql_parallel_migrate(self, from_table, to_table, final_parallel, parallel_key=None, parallel_method='multiprocess'):
        #分批数据
        from_db = self.source_db_info.get('db')
        to_db = self.target_db_info.get('db')
        #Mysql_Operate = MysqlSourceTarget(self.source_db_info, self.target_db_info)
        sql_max_min = 'select max(`' + parallel_key + '`), min(`' + parallel_key + '`) from `' + from_db + '`.`' + from_table + '`'
        res_max_min = self.mysql_source.mysql_source_data(sql_max_min)
        max_key = res_max_min[0][0]
        min_key = res_max_min[0][1]
        #per_rows为每进程处理的数据量
        per_rows = math.ceil((max_key - min_key + 1)/final_parallel)

        #源库select语句
        sql_select_list = []
        for p in range(final_parallel):
            from_rowid = min_key + (p * per_rows)
            to_rowid = min_key + ((p + 1) * per_rows)
            sql_info = {'from_db': from_db, 'from_table': from_table, 'to_table': to_table, 'parallel_key': parallel_key, 'per_rows': per_rows, 'from_rowid': from_rowid, 'to_rowid': to_rowid, 'max_key': max_key}
            sql_select_list.append(sql_info)
            #sql_select = 'select /*!40001 SQL_NO_CACHE */ * from `' + from_db + '`.`' + from_table + '` where `' + parallel_key + '` >= ' + \
            #             str(min_key + (p * per_rows)) + ' and `' + parallel_key + '` < ' + str(min_key + ((p + 1) * per_rows))
            #select_insert_dict = {'table_name': to_table, 'sql_statement': sql_select}
            #sql_select_list.append(select_insert_dict)
        print(sql_select_list)

        print('[DBM] Inserting data into table `' + to_table + '`')
        #多进程
        if parallel_method == 'multiprocess':
            with Pool(final_parallel) as p:
                insert_rows = p.map(partial(mysql_select_insert, source_db_info=self.source_db_info, target_db_info=self.target_db_info), sql_select_list)
            total_rows = reduce(lambda x, y: x + y, insert_rows)
            print(insert_rows)
            print(total_rows)
        else:
        #多线程
            thread_list = []
            result_list = []
            for t in range(final_parallel):
                sql_info = sql_select_list[t]
                #sthread = threading.Thread(target=mysql_select_insert, args=(sql_info,self.source_db_info, self.target_db_info), name=('thread-' + str(t)))
                sthread = MyThread(mysql_select_insert, (sql_info,self.source_db_info, self.target_db_info))
                thread_list.append(sthread)
            for t in thread_list:
                t.start()
            for t in thread_list:
                t.join()
            for t in thread_list:
                result_list.append(sthread.get_result())
            total_rows = reduce(lambda x, y: x + y, result_list)
        return total_rows

    #mysql数据同步串行/并行选择
    def mysql_parallel_flag(self, from_table, res_tablestatus, res_columns, parallel=0):
        #用户指定并发数或行数大于10w或空间使用大于10M，并且主键列为int时，设置并发
        parallel_flag = 0 if parallel == 0 else 1
        pri_keys = [(record.get('field'), record.get('type')) for record in res_columns if
                    (record['key'] == 'PRI' or record['key'] == 'pri')]
        pri_key_types = [pri_key[1] for pri_key in pri_keys]
        if (res_tablestatus[0].get('rows') > 100000 or res_tablestatus[0].get('data_length') > 10000000) and 'int' in [key_type for key_type in pri_key_types][0]:
            #并行主键
            parallel_key = [k[0] for k in pri_keys if 'int' in k[1]][0]
            parallel_flag = 1 if parallel_key else 0
        else:
            parallel_key = None
            parallel_flag = 0
        # 设置并行方式(linux为多进程，windows为多线程) 和 最大并发数, （多线程速度较慢，这里全改为多进程）
        #parallel_method = 'multiprocess' if os.name == 'posix' else 'threading'
        parallel_method = 'multiprocess'
        max_parallel = cpu_count()

        #计算实际并发数=用户指定并发数；如未指定，实际并发数=round(表记录数/10w)
        if parallel == 0:
            auto_parallel = round(int(res_tablestatus[0].get('rows'))/100000)
            final_parallel = min(auto_parallel, max_parallel)
        else:
            final_parallel = min(parallel, max_parallel)

        return parallel_flag, final_parallel, parallel_key, parallel_method

