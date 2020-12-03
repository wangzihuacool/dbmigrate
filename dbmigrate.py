# -*- code: utf-8 -*-

'''
数据库迁移主程序
NOTE: 增加mysql的基于where条件的增量数据同步 updated by wl_lw at 20200411
'''

import traceback
import time, sys, os, stat
import requests
from math import ceil, floor
from multiprocessing import cpu_count, Process, Pool, freeze_support, Manager
from functools import reduce, partial
from env import *
from mysql_migrate import MysqlSource, MysqlTarget, MysqlDataMigrate, MysqlMetadataMapping
from oracle_migrate import OracleSource, OracleTarget, OracleDataMigrate
from mssql_migrate import MssqlSource, MssqlTarget, MssqlDataMigrate


# mysql -> mysql 数据库级别同步(全库元数据+数据)基础同步方法
def mysql_db_all_migrate(from_table, source_db_info, target_db_info, p_mysql_source=None, p_mysql_target=None):
    mysql_source = MysqlSource(**source_db_info) if not p_mysql_source else p_mysql_source
    mysql_target = MysqlTarget(**target_db_info) if not p_mysql_target else p_mysql_target
    res_tablestatus, res_createtable, res_columns, res_triggers = mysql_source.mysql_source_table(from_table)
    index_column_info = mysql_source.mysql_source_index(from_table)
    # 目标表
    exist_table_list = mysql_target.mysql_target_exist_tables()
    to_table = from_table
    # 标准处理
    # 创建表
    mysql_target.mysql_target_table(to_table, table_exists_action, res_columns=res_columns,
                                    res_tablestatus=res_tablestatus)
    # 创建索引
    mysql_target.mysql_target_index(to_table, index_column_info)
    # 同步数据
    mysql_dbm = MysqlDataMigrate(source_db_info, target_db_info)
    parallel_flag, final_parallel, parallel_key, parallel_method = mysql_dbm.mysql_parallel_flag(from_table,
                                                                                                 res_tablestatus,
                                                                                                 res_columns,
                                                                                                 parallel=parallel)
    if parallel_flag == 0:
        total_rows = mysql_dbm.mysql_serial_migrate(from_table, to_table)
    else:
        total_rows = mysql_dbm.mysql_parallel_migrate(from_table, to_table, final_parallel, parallel_key=parallel_key,
                                                      parallel_method=parallel_method)
    print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
    # trigger同步
    # to_do
    if not p_mysql_source:
        mysql_source.close()
    if not p_mysql_target:
        mysql_target.close()


# mysql -> mysql 数据库同步时表并行同步的处理方法
def mysql_db_migrate_parallel_subprocess(task_queue, source_db_info, target_db_info):
    mysql_source = MysqlSource(**source_db_info)
    mysql_target = MysqlTarget(**target_db_info)
    # 接受queue队列中的table进行处理
    while True:
        if task_queue.empty():
            break
        table_name = task_queue.get()
        try:
            # current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # print('[DBM] Debug: ' + current_time + ' Process ' + str(os.getpid()) + ' is migrating table ' + table_name + '.')
            mysql_db_all_migrate(table_name, source_db_info, target_db_info, p_mysql_source=mysql_source, p_mysql_target=mysql_target)
            # current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # print('[DBM] Debug: ' + current_time + ' Process ' + str(os.getpid()) + ' finish migrating table ' + table_name + '.')
        except:
            print('[DBM] Error: table ' + table_name + ' migrate failed')
            traceback.print_exc()
            continue
        task_queue.task_done()
    # mysql_source.close()
    # mysql_target.close()


# mysql -> mysql 数据库同步时表并行同步的调度方法
def mysql_db_migrate_parallel_schedule(task_list, num_of_processes=2):
    manager = Manager()
    task_queue = manager.Queue()
    for task in task_list:
        task_queue.put(task)
    for n in range(num_of_processes):
        p = Process(target=mysql_db_migrate_parallel_subprocess, args=(task_queue, source_db_info, target_db_info))
        p.start()
    print('[DBM] Enable parallel job for dbmigrate, parallel num is ' + str(num_of_processes) + '.')
    task_queue.join()
    p.join()


# mysql -> mysql 数据库同步
def mysql_to_mysql():
    # mysql检查源库和源表,检查目标库
    mysql_source = MysqlSource(**source_db_info)
    mysql_target = MysqlTarget(**target_db_info)
    mysql_source.source_db_check()
    from_tables, migrate_granularity = mysql_source.source_table_check(*source_tables, content=content)
    mysql_target.mysql_target_createdb(migrate_granularity, **target_db_info)
    target_tables = from_tables
    mysql_source.close()
    mysql_target.close()

    # mysql -> mysql 数据库级别同步
    if migrate_granularity == 'db':
        # 同步全库所有元数据+数据
        if content == 'all':
            # 表同步(根据是否开启性能模式来选择是否需要表之间并行同步，开启表并行时默认最大并发为8，也可手动设置)
            # 动态计算表迁移并行度
            if performance_mode and performance_mode == 1:
                table_parallel = int(min(round(len(from_tables)/50), cpu_count()/2, 8))
                # n = ceil(len(from_tables)/table_parallel)
                # from_tables_list = [from_tables[i:i+n] for i in range(0, len(from_tables), n)]
                mysql_db_migrate_parallel_schedule(from_tables, table_parallel)
            # 手动指定表迁移并行度
            elif performance_mode and performance_mode > 1:
                table_parallel = performance_mode
                mysql_db_migrate_parallel_schedule(from_tables, table_parallel)
            # 不使用表迁移并行
            else:
                for from_table in from_tables:
                    mysql_db_all_migrate(from_table, source_db_info, target_db_info, mysql_source=mysql_source, mysql_target=mysql_target)
            mysql_source = MysqlSource(**source_db_info)
            mysql_target = MysqlTarget(**target_db_info)
            # 外键同步
            final_fk = mysql_source.mysql_source_fk(from_tables)
            mysql_target.mysql_target_fk(final_fk)
            # 视图同步, 存储过程同步, 函数同步
            from_views_ddl, from_views_tmp_ddl, from_procedures_ddl, from_functions_ddl, from_routines, from_events = mysql_source.mysql_source_pkg()
            if from_views_ddl:
                mysql_target.mysql_target_view_tmp(from_views_tmp_ddl)
                mysql_target.mysql_target_view(from_views_ddl)
            if from_procedures_ddl:
                mysql_target.mysql_target_procedure(from_procedures_ddl)
            if from_functions_ddl:
                mysql_target.mysql_target_procedure(from_functions_ddl)
            mysql_source.close()
            mysql_target.close()

        elif content == 'metadata':
            # 表同步
            mysql_source = MysqlSource(**source_db_info)
            mysql_target = MysqlTarget(**target_db_info)
            for from_table in from_tables:
                res_tablestatus, res_createtable, res_columns, res_triggers = mysql_source.mysql_source_table(from_table)
                index_column_info = mysql_source.mysql_source_index(from_table)
                # 目标表
                exist_table_list = mysql_target.mysql_target_exist_tables()
                to_table = from_table
                # 标准处理
                # 创建表
                mysql_target.mysql_target_table(to_table, table_exists_action, res_columns=res_columns,
                                                res_tablestatus=res_tablestatus)
                # 创建索引
                mysql_target.mysql_target_index(to_table, index_column_info)

                # trigger同步
                # to_do
            # 外键同步
            final_fk = mysql_source.mysql_source_fk(from_tables)
            mysql_target.mysql_target_fk(final_fk)

            # 视图同步, 存储过程同步, 函数同步
            # 视图同步时为解决视图间存在依赖关系导致创建视图顺序无法确定的问题，先创建跟视图同名的临时表，然后删除临时表创建视图，解决创建视图时表不存在的问题。
            # 视图同步方式参考mysqldump的处理方法。
            from_views_ddl, from_views_tmp_ddl, from_procedures_ddl, from_functions_ddl, from_routines, from_events = mysql_source.mysql_source_pkg()
            if from_views_ddl:
                mysql_target.mysql_target_view_tmp(from_views_tmp_ddl)
                mysql_target.mysql_target_view(from_views_ddl)
            if from_procedures_ddl:
                mysql_target.mysql_target_procedure(from_procedures_ddl)
            if from_functions_ddl:
                mysql_target.mysql_target_function(from_functions_ddl)
            mysql_source.close()
            mysql_target.close()

        # 同步全库所有表非主键索引
        elif content == 'index':
            print('DBM Error 200: mysql数据库索引同步在table级别实现。')
            sys.exit(1)
            #for from_table in from_tables:
            #    index_column_info = mysql_source.mysql_source_index(from_table)
            #    # 目标表
            #    exist_table_list = mysql_target.mysql_target_exist_tables()
            #    to_table = from_table
            #    if to_table in exist_table_list:
            #        mysql_target.mysql_drop_index(to_table)
            #        mysql_target.mysql_target_index(to_table, index_column_info)

        elif content == 'data':
            print('[DBM] Error 100 : 参数错误，content=\'data\' 仅适用于表同步.')
            sys.exit(1)
        else:
            print('[DBM] Error 100 : 参数错误，content=%s.' % content)
            sys.exit(1)
    # mysql -> mysql 表级别同步
    elif migrate_granularity == 'table':
        mysql_source = MysqlSource(**source_db_info)
        mysql_target = MysqlTarget(**target_db_info)
        # 同步所有表元数据+数据
        if content == 'all':
            #表同步
            for from_table in from_tables:
                res_tablestatus, res_createtable, res_columns, res_triggers = mysql_source.mysql_source_table(from_table)
                index_column_info = mysql_source.mysql_source_index(from_table)
                # 目标表
                exist_table_list = mysql_target.mysql_target_exist_tables()
                to_table = from_table
                if to_table in exist_table_list and table_exists_action == 'drop':
                    # 删除目标表
                    mysql_target.mysql_target_execute_no_trans('drop table if exists `' + to_db + '`.`' + to_table + '`')
                    # 创建表
                    mysql_target.mysql_target_table(to_table, table_exists_action, res_columns=res_columns,
                                                    res_tablestatus=res_tablestatus)
                    # 创建索引
                    mysql_target.mysql_target_index(to_table, index_column_info)
                    # 同步数据
                    mysql_dbm = MysqlDataMigrate(source_db_info, target_db_info)
                    parallel_flag, final_parallel, parallel_key, parallel_method = mysql_dbm.mysql_parallel_flag(from_table,
                                                                                                                 res_tablestatus,
                                                                                                                 res_columns,
                                                                                                                 parallel=parallel)
                    if parallel_flag == 0:
                        total_rows = mysql_dbm.mysql_serial_migrate(from_table, to_table)
                    else:
                        total_rows = mysql_dbm.mysql_parallel_migrate(from_table, to_table, final_parallel,
                                                                      parallel_key=parallel_key,
                                                                      parallel_method=parallel_method)
                    print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')

                    # trigger同步
                    # to_do

                elif to_table in exist_table_list and table_exists_action == 'truncate':
                    # truncate目标表
                    mysql_target.mysql_target_execute_no_trans('truncate table `' + to_db + '`.`' + to_table + '`')
                    # 同步数据
                    mysql_dbm = MysqlDataMigrate(source_db_info, target_db_info)
                    parallel_flag, final_parallel, parallel_key, parallel_method = mysql_dbm.mysql_parallel_flag(from_table, res_tablestatus, res_columns, parallel=parallel)
                    if parallel_flag == 0:
                        total_rows = mysql_dbm.mysql_serial_migrate(from_table, to_table)
                    else:
                        total_rows = mysql_dbm.mysql_parallel_migrate(from_table, to_table, final_parallel, parallel_key=parallel_key, parallel_method=parallel_method)
                    print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')

                elif to_table in exist_table_list and table_exists_action == 'append':
                    # 同步数据
                    mysql_dbm = MysqlDataMigrate(source_db_info, target_db_info)
                    parallel_flag, final_parallel, parallel_key, parallel_method = mysql_dbm.mysql_parallel_flag(from_table,
                                                                                                                 res_tablestatus,
                                                                                                                 res_columns,
                                                                                                                 parallel=parallel)
                    if parallel_flag == 0:
                        total_rows = mysql_dbm.mysql_serial_migrate(from_table, to_table)
                    else:
                        total_rows = mysql_dbm.mysql_parallel_migrate(from_table, to_table, final_parallel,
                                                                      parallel_key=parallel_key,
                                                                      parallel_method=parallel_method)
                    print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')

                elif to_table in exist_table_list and table_exists_action == 'skip':
                    continue

                elif to_table not in exist_table_list:
                    # 表在目标库不存在，标准处理
                    # 创建表
                    mysql_target.mysql_target_table(to_table, table_exists_action, res_columns=res_columns,
                                                    res_tablestatus=res_tablestatus)
                    # 创建索引
                    mysql_target.mysql_target_index(to_table, index_column_info)
                    # 同步数据
                    mysql_dbm = MysqlDataMigrate(source_db_info, target_db_info)
                    parallel_flag, final_parallel, parallel_key, parallel_method = mysql_dbm.mysql_parallel_flag(from_table,
                                                                                                                 res_tablestatus,
                                                                                                                 res_columns,
                                                                                                                 parallel=parallel)
                    if parallel_flag == 0:
                        total_rows = mysql_dbm.mysql_serial_migrate(from_table, to_table)
                    else:
                        total_rows = mysql_dbm.mysql_parallel_migrate(from_table, to_table, final_parallel,
                                                                      parallel_key=parallel_key,
                                                                      parallel_method=parallel_method)
                    print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')

                    # trigger同步
                    # to_do
                else:
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    sys.exit(1)

            # 外键同步
            final_fk = mysql_source.mysql_source_fk(from_tables)
            mysql_target.mysql_target_fk(final_fk)

        # 表元数据同步
        elif content == 'metadata':
            #表同步
            for from_table in from_tables:
                res_tablestatus, res_createtable, res_columns, res_triggers = mysql_source.mysql_source_table(from_table)
                index_column_info = mysql_source.mysql_source_index(from_table)
                # 目标表
                exist_table_list = mysql_target.mysql_target_exist_tables()
                to_table = from_table
                if to_table in exist_table_list and table_exists_action == 'drop':
                    # 删除目标表
                    mysql_target.mysql_target_execute_no_trans('drop table if exists `' + to_db + '`.`' + to_table + '`')
                    # 创建表
                    mysql_target.mysql_target_table(to_table, table_exists_action, res_columns=res_columns,
                                                    res_tablestatus=res_tablestatus)
                    # 创建索引
                    mysql_target.mysql_target_index(to_table, index_column_info)

                    # trigger同步
                    # to_do
                elif to_table in exist_table_list and table_exists_action == 'truncate':
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    sys.exit(1)
                elif to_table in exist_table_list and table_exists_action == 'append':
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    sys.exit(1)
                elif to_table in exist_table_list and table_exists_action == 'skip':
                    continue
                elif to_table not in exist_table_list:
                    # 创建表
                    mysql_target.mysql_target_table(to_table, table_exists_action, res_columns=res_columns,
                                                    res_tablestatus=res_tablestatus)
                    # 创建索引
                    mysql_target.mysql_target_index(to_table, index_column_info)

                    # trigger同步
                    # to_do
                else:
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    sys.exit(1)
            # 外键同步
            final_fk = mysql_source.mysql_source_fk(from_tables)
            mysql_target.mysql_target_fk(final_fk)

        elif content == 'data':
            #只同步表数据
            for from_table in from_tables:
                res_tablestatus, res_createtable, res_columns, res_triggers = mysql_source.mysql_source_table(from_table)
                index_column_info = mysql_source.mysql_source_index(from_table)
                # 目标表
                exist_table_list = mysql_target.mysql_target_exist_tables()
                to_table = from_table
                if to_table in exist_table_list and table_exists_action == 'drop':
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    sys.exit(1)
                elif to_table in exist_table_list and table_exists_action == 'truncate':
                    # truncate目标表
                    mysql_target.mysql_target_execute_no_trans('truncate table `' + to_db + '`.`' + to_table + '`')
                    # 同步数据
                    mysql_dbm = MysqlDataMigrate(source_db_info, target_db_info)
                    parallel_flag, final_parallel, parallel_key, parallel_method = mysql_dbm.mysql_parallel_flag(from_table,
                                                                                                                 res_tablestatus,
                                                                                                                 res_columns,
                                                                                                                 parallel=parallel)
                    if parallel_flag == 0:
                        total_rows = mysql_dbm.mysql_serial_migrate(from_table, to_table)
                    else:
                        total_rows = mysql_dbm.mysql_parallel_migrate(from_table, to_table, final_parallel,
                                                                      parallel_key=parallel_key,
                                                                      parallel_method=parallel_method)
                    print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                elif to_table in exist_table_list and table_exists_action == 'append':
                    # 同步数据
                    mysql_dbm = MysqlDataMigrate(source_db_info, target_db_info)
                    parallel_flag, final_parallel, parallel_key, parallel_method = mysql_dbm.mysql_parallel_flag(
                        from_table,
                        res_tablestatus,
                        res_columns,
                        parallel=parallel)
                    if parallel_flag == 0:
                        total_rows = mysql_dbm.mysql_serial_migrate(from_table, to_table)
                    else:
                        total_rows = mysql_dbm.mysql_parallel_migrate(from_table, to_table, final_parallel,
                                                                      parallel_key=parallel_key,
                                                                      parallel_method=parallel_method)
                    print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                elif to_table in exist_table_list and table_exists_action == 'skip':
                    print('[DBM] table ' + to_table + ' skiped due to table_exists_action == skip')
                else:
                    print('[DBM] error 101 : 目标表[%s]不存在' % to_table)

        # 同步表上非主键索引
        elif content == 'index':
            for from_table in from_tables:
                index_column_info = mysql_source.mysql_source_index(from_table)
                # 目标表
                exist_table_list = mysql_target.mysql_target_exist_tables()
                to_table = from_table
                if to_table in exist_table_list:
                    mysql_target.mysql_drop_index(to_table)
                    mysql_target.mysql_target_index(to_table, index_column_info)

        elif content == 'increment':
            # 增量同步表数据, added by wl_lw at 20200411
            if not incremental_method or incremental_method.lower == 'none':
                print('[DBM] error 100: 参数错误, incremental_method=%s 参数错误.' % incremental_method)
                sys.exit(1)
            for from_table in from_tables:
                res_tablestatus, res_createtable, res_columns, res_triggers = mysql_source.mysql_source_table(from_table)
                index_column_info = mysql_source.mysql_source_index(from_table)
                # 目标表
                exist_table_list = mysql_target.mysql_target_exist_tables()
                to_table = from_table
                if to_table in exist_table_list and table_exists_action == 'drop':
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    sys.exit(1)
                elif to_table in exist_table_list and table_exists_action == 'truncate':
                    # truncate目标表
                    print('[DBM] truncate table ' + to_table)
                    numrows = mysql_target.mysql_target_execute_no_trans('truncate table `' + to_db + '`.`' + to_table + '`')
                    # 同步数据, 增量同步目前只支持串行
                    mysql_dbm = MysqlDataMigrate(source_db_info, target_db_info)
                    # parallel_flag, final_parallel, parallel_key, parallel_method = mysql_dbm.mysql_parallel_flag(from_table, res_tablestatus, res_columns, parallel=parallel)
                    if incremental_method == 'where':
                        if not where_clause:
                            print('[DBM] note 300: 增量同步方式为where，但未指定where_clause，将启用全量同步')
                        elif not (where_clause.startswith('where') or where_clause.startswith('WHERE')):
                            print('[DBM] error 100: 参数错误, where_clause=%s 参数错误，where_clause必须以where开头.' % where_clause)
                        total_rows = mysql_dbm.mysql_incr_serial_migrate(from_table, to_table, incremental_method=incremental_method, where_clause=where_clause)
                        print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                    else:
                        print('[DBM] error 100 : 参数错误，incremental_method=%s 参数错误，目前仅支持where_clause方式.' % incremental_method)
                elif to_table in exist_table_list and table_exists_action == 'append':
                    # 同步数据, 增量同步目前只支持串行
                    mysql_dbm = MysqlDataMigrate(source_db_info, target_db_info)
                    # parallel_flag, final_parallel, parallel_key, parallel_method = mysql_dbm.mysql_parallel_flag(from_table, res_tablestatus, res_columns, parallel=parallel)
                    if incremental_method == 'where':
                        if not where_clause:
                            print('[DBM] note 300: 增量同步方式为where，但未指定where_clause，将启用全量同步')
                        elif not (where_clause.startswith('where') or where_clause.startswith('WHERE')):
                            print('[DBM] error 100: 参数错误, where_clause=%s 参数错误，where_clause必须以where开头.' % where_clause)
                        total_rows = mysql_dbm.mysql_incr_serial_migrate(from_table, to_table, incremental_method=incremental_method, where_clause=where_clause)
                        print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                    else:
                        print('[DBM] error 100 : 参数错误，incremental_method=%s 参数错误，目前仅支持where_clause方式.' % incremental_method)
                elif to_table in exist_table_list and table_exists_action == 'skip':
                    print('[DBM] table ' + to_table + ' skiped due to table_exists_action == skip')
                else:
                    print('[DBM] error 101 : 目标表[%s]不存在' % to_table)
        else:
            print('[DBM] error 100 : content=%s 参数错误.' % content)
        mysql_source.close()
        mysql_target.close()
    else:
        print('[DBM] error 100: source_tables=%s 参数错误.' % source_tables)
        sys.exit(1)


# mysql -> oracle 数据库同步
def mysql_to_oracle():
    # 检查源库mysql和目标库oracle, 检查源表是否存在
    mysql_source = MysqlSource(**source_db_info)
    oracle_target = OracleTarget(**target_db_info)
    mysql_source.source_db_check()
    from_tables, migrate_granularity = mysql_source.source_table_check(*source_tables, content=content)
    target_tables = from_tables

    # mysql -> mysql 数据库级别同步
    if migrate_granularity == 'db':
        print("[DBM] error 999 : 目前Mysql->Oracle的异构数据库同步只支持表级别的数据同步(content='data')!")
        sys.exit(1)
    elif migrate_granularity == 'table':
        # 只同步表数据
        if content == 'data':
            for from_table in from_tables:
                from_table = from_table.lower()
                res_tablestatus, res_createtable, res_columns, res_triggers = mysql_source.mysql_source_table(from_table)
                index_column_info = mysql_source.mysql_source_index(from_table)
                # 检查目标表是否已存在
                exist_table_list = oracle_target.oracle_target_exist_tables()
                to_table = from_table
                if to_table in exist_table_list and table_exists_action == 'drop':
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    sys.exit(1)
                elif to_table in exist_table_list and table_exists_action == 'truncate':
                    # 先truncate目标表，然后追加数据
                    truncate_sql = 'truncate table ' + to_table
                    oracle_target.oracle_execute_dml(truncate_sql)
                    # 同步数据, 源库是mysql，执行MysqlDataMigrate
                    mysql_dbm = MysqlDataMigrate(source_db_info, target_db_info)
                    parallel_flag, final_parallel, parallel_key, parallel_method = mysql_dbm.mysql_parallel_flag(
                        from_table,
                        res_tablestatus,
                        res_columns,
                        parallel=parallel)
                    if parallel_flag == 0:
                        total_rows = mysql_dbm.mysql_serial_migrate(from_table, to_table)
                    else:
                        total_rows = mysql_dbm.mysql_parallel_migrate(from_table, to_table, final_parallel,
                                                                      parallel_key=parallel_key,
                                                                      parallel_method=parallel_method)
                    print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                elif to_table in exist_table_list and table_exists_action == 'append':
                    # 数据追加到目标表
                    mysql_dbm = MysqlDataMigrate(source_db_info, target_db_info)
                    parallel_flag, final_parallel, parallel_key, parallel_method = mysql_dbm.mysql_parallel_flag(
                        from_table,
                        res_tablestatus,
                        res_columns,
                        parallel=parallel)
                    if parallel_flag == 0:
                        total_rows = mysql_dbm.mysql_serial_migrate(from_table, to_table)
                    else:
                        total_rows = mysql_dbm.mysql_parallel_migrate(from_table, to_table, final_parallel,
                                                                      parallel_key=parallel_key,
                                                                      parallel_method=parallel_method)
                    print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                elif to_table in exist_table_list and table_exists_action == 'skip':
                    print('[DBM] table ' + to_table + ' skiped due to table_exists_action == skip')
                else:
                    print('[DBM] error 101 : 目标表[%s]不存在' % to_table)
        else:
            print("[DBM] error 999 : 目前Mysql->Oracle的异构数据库同步只支持表级别的数据同步(content='data')!")
            sys.exit(1)
    else:
        print('[DBM] error 100: source_tables=%s 参数错误.' % source_tables)
        sys.exit(1)


# oracle -> oracle 数据库同步
def oracle_to_oracle():
    # 检查源库oracle和目标库oracle, 检查源表是否存在
    oracle_source = OracleSource(**source_db_info)
    oracle_target = OracleTarget(**target_db_info)
    from_tables, migrate_granularity = oracle_source.source_table_check(*source_tables)
    to_tables = from_tables

    # oracle -> oracle 数据库级别同步
    if migrate_granularity == 'db':
        print("[DBM] error 999 : 目前Oracle->Oracle的数据库同步只支持表级别的数据同步(content='data')!")
        sys.exit(1)
    # oracle -> oracle 表级别同步
    elif migrate_granularity == 'table':
        # 只同步表数据
        if content == 'data':
            # 同步数据前禁用外键
            sql_enable_constraints = oracle_target.oracle_disable_constraint(to_tables)
            for from_table in from_tables:
                from_table = from_table.lower()
                res_tablestatus, res_partitions, res_columns, res_triggers, res_segments = oracle_source.oracle_source_table(from_table)
                index_column_info = oracle_source.oracle_source_index(from_table)
                # 检查目标表是否已存在
                exist_table_list = oracle_target.oracle_target_exist_tables()
                to_table = from_table
                if to_table in exist_table_list and table_exists_action == 'drop':
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    sys.exit(1)
                elif to_table in exist_table_list and table_exists_action == 'truncate':
                    # 先truncate目标表，然后追加数据
                    truncate_sql = 'truncate table ' + to_table
                    oracle_target.oracle_execute_dml(truncate_sql)
                    # 同步数据, 源库是oracle，执行OracleDataMigrate
                    oracle_dbm = OracleDataMigrate(source_db_info, target_db_info)
                    parallel_flag, final_parallel, parallel_key, parallel_method, lob_flag = oracle_dbm.oracle_parallel_flag(
                        from_table, index_column_info, res_columns, res_segments, parallel=parallel)
                    if parallel_flag == 0:
                        total_rows = oracle_dbm.oracle_serial_migrate(from_table, to_table)
                    else:
                        total_rows = oracle_dbm.oracle_parallel_migrate(from_table, to_table, final_parallel,
                                                                        parallel_key=parallel_key,
                                                                        parallel_method=parallel_method,
                                                                        lob_flag=lob_flag)
                    print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                elif to_table in exist_table_list and table_exists_action == 'append':
                    # 数据追加到目标表
                    oracle_dbm = OracleDataMigrate(source_db_info, target_db_info)
                    parallel_flag, final_parallel, parallel_key, parallel_method, lob_flag = oracle_dbm.oracle_parallel_flag(
                        from_table, index_column_info, res_columns, res_segments, parallel=0)
                    if parallel_flag == 0:
                        total_rows = oracle_dbm.oracle_serial_migrate(from_table, to_table)
                    else:
                        total_rows = oracle_dbm.oracle_parallel_migrate(from_table, to_table, final_parallel,
                                                                        parallel_key=parallel_key,
                                                                        parallel_method=parallel_method,
                                                                        lob_flag=lob_flag)
                    print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                elif to_table in exist_table_list and table_exists_action == 'skip':
                    print('[DBM] table ' + to_table + ' skiped due to table_exists_action == skip')
                else:
                    print('[DBM] error 101 : 目标表[%s]不存在' % to_table)
            # 同步后启用外键
            oracle_target.oracle_enable_constraint(sql_enable_constraints)
        # 带where条件的同步
        elif content == 'increment':
            # 同步数据前禁用外键
            sql_enable_constraints = oracle_target.oracle_disable_constraint(to_tables)
            for from_table in from_tables:
                from_table = from_table.lower()
                res_tablestatus, res_partitions, res_columns, res_triggers, res_segments = oracle_source.oracle_source_table(
                    from_table)
                index_column_info = oracle_source.oracle_source_index(from_table)
                # 检查目标表是否已存在
                exist_table_list = oracle_target.oracle_target_exist_tables()
                to_table = from_table
                if to_table in exist_table_list and table_exists_action == 'drop':
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    sys.exit(1)
                elif to_table in exist_table_list and table_exists_action == 'truncate':
                    # 先truncate目标表，然后追加数据
                    truncate_sql = 'truncate table ' + to_table
                    oracle_target.oracle_execute_dml(truncate_sql)
                    # 同步数据, 源库是oracle，执行OracleDataMigrate，增量同步目前只支持串行
                    oracle_dbm = OracleDataMigrate(source_db_info, target_db_info)
                    if incremental_method == 'where':
                        if not where_clause:
                            print('[DBM] note 300: 增量同步方式为where，但未指定where_clause，将启用全量同步')
                        elif not (where_clause.startswith('where') or where_clause.startswith('WHERE')):
                            print('[DBM] error 100: 参数错误, where_clause=%s 参数错误，where_clause必须以where开头.' % where_clause)
                        total_rows = oracle_dbm.oracle_incr_serial_migrate(from_table, to_table,
                                                                           incremental_method=incremental_method,
                                                                           where_clause=where_clause)
                        print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                    else:
                        print('[DBM] error 100 : 参数错误，incremental_method=%s 参数错误，目前仅支持where_clause方式.' % incremental_method)
                elif to_table in exist_table_list and table_exists_action == 'append':
                    # 同步数据, 源库是oracle，执行OracleDataMigrate，增量同步目前只支持串行
                    oracle_dbm = OracleDataMigrate(source_db_info, target_db_info)
                    if incremental_method == 'where':
                        if not where_clause:
                            print('[DBM] note 300: 增量同步方式为where，但未指定where_clause，将启用全量同步')
                        elif not (where_clause.startswith('where') or where_clause.startswith('WHERE')):
                            print('[DBM] error 100: 参数错误, where_clause=%s 参数错误，where_clause必须以where开头.' % where_clause)
                        total_rows = oracle_dbm.oracle_incr_serial_migrate(from_table, to_table,
                                                                           incremental_method=incremental_method,
                                                                           where_clause=where_clause)
                        print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                    else:
                        print(
                            '[DBM] error 100 : 参数错误，incremental_method=%s 参数错误，目前仅支持where_clause方式.' % incremental_method)
                elif to_table in exist_table_list and table_exists_action == 'skip':
                    print('[DBM] table ' + to_table + ' skiped due to table_exists_action == skip')
                else:
                    print('[DBM] error 101 : 目标表[%s]不存在' % to_table)
            # 同步后启用外键
            oracle_target.oracle_enable_constraint(sql_enable_constraints)
        else:
            print("[DBM] error 999 : 目前Oracle->Oracle的数据库同步只支持表级别的数据同步(content='data' or content='increment')!")
            sys.exit(1)
    else:
        print('[DBM] error 100: source_tables=%s 参数错误.' % source_tables)
        sys.exit(1)


# oracle -> mysql 数据库同步
def oracle_to_mysql():
    # 检查源库oracle和目标库mysql, 检查源表是否存在
    oracle_source = OracleSource(**source_db_info)
    mysql_target = MysqlTarget(**target_db_info)
    from_tables, migrate_granularity = oracle_source.source_table_check(*source_tables)
    to_tables = from_tables

    # oracle -> mysql 数据库级别同步
    if migrate_granularity == 'db':
        print("[DBM] error 999 : 目前Oracle->Mysql的数据库同步只支持表级别的同步!")
        sys.exit(1)
    elif migrate_granularity == 'table':
        # oracle->mysql表同步
        if content == 'all':
            # 同步数据前禁用外键，在MysqlTarget类初始化时禁用外键
            # mysql_target.mysql_target_execute('set foreign_key_checks=0')
            # 先同步元数据
            source_type = source_db_info.get('source_db_type')
            res_pk, res_fk = oracle_source.oracle_source_pk_fk(from_tables)
            for from_table in from_tables:
                # oracle源表
                res_tablestatus, res_partitions, res_columns, res_triggers, res_segments = oracle_source.oracle_source_table(from_table)
                index_column_info = oracle_source.oracle_source_index(from_table)
                res_comments = oracle_source.oracle_source_comment(from_table)
                # oracle->mysql的转换
                mysql_convert = MysqlMetadataMapping(source_type, from_table, res_pk)
                mysql_columns = mysql_convert.column_convert(res_columns, res_comments)
                mysql_tablestatus = mysql_convert.table_comment_convert(res_comments)
                mysql_indexes = mysql_convert.index_convert(index_column_info, res_columns)
                # 检查mysql目标表是否存在
                exist_table_list = mysql_target.mysql_target_exist_tables()
                to_table = from_table
                if to_table in exist_table_list and table_exists_action == 'drop':
                    # 删除目标表
                    mysql_target.mysql_target_execute_no_trans(
                        'drop table if exists `' + to_db + '`.`' + to_table + '`')
                    # 创建表
                    mysql_target.mysql_target_table(to_table, table_exists_action, res_columns=mysql_columns,
                                                    res_tablestatus=mysql_tablestatus)
                    # 创建索引
                    mysql_target.mysql_target_index(to_table, mysql_indexes)
                    # oracle->mysql的trigger同步(需要么?)
                    # to_do
                elif to_table in exist_table_list and table_exists_action == 'truncate':
                    # 先truncate目标表
                    print('[DBM] Truncate table `' + to_table + '`')
                    mysql_target.mysql_target_execute_no_trans('truncate table `' + to_db + '`.`' + to_table + '`')

                elif to_table in exist_table_list and table_exists_action == 'append':
                    pass
                elif to_table in exist_table_list and table_exists_action == 'skip':
                    continue
                elif to_table not in exist_table_list:
                    # 创建表
                    mysql_target.mysql_target_table(to_table, table_exists_action, res_columns=mysql_columns,
                                                    res_tablestatus=mysql_tablestatus)
                    # 创建索引
                    mysql_target.mysql_target_index(to_table, mysql_indexes)
                    # oracle->mysql的trigger同步(需要么?)
                    # to_do
                else:
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    sys.exit(1)

                # 同步数据, 源库是oracle，执行OracleDataMigrate
                oracle_dbm = OracleDataMigrate(source_db_info, target_db_info)
                parallel_flag, final_parallel, parallel_key, parallel_method, lob_flag = oracle_dbm.oracle_parallel_flag(
                    from_table, index_column_info, res_columns, res_segments, parallel=parallel)
                if parallel_flag == 0:
                    total_rows = oracle_dbm.oracle_serial_migrate(from_table, to_table)
                else:
                    total_rows = oracle_dbm.oracle_parallel_migrate(from_table, to_table, final_parallel,
                                                                    parallel_key=parallel_key,
                                                                    parallel_method=parallel_method,
                                                                    lob_flag=lob_flag)
                print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
            # 外键同步
            mysql_fk = MysqlMetadataMapping.fk_convert(res_fk)
            mysql_target.mysql_target_fk(mysql_fk)
            # 同步后启用外键, 在MysqlTarge类关闭时禁用外键
            # mysql_target.mysql_target_execute('set foreign_key_checks=1')

        # 表的元数据同步
        elif content == 'metadata':
            # 同步数据前禁用外键，在MysqlTarget类初始化时禁用外键
            # mysql_target.mysql_target_execute('set foreign_key_checks=0')
            source_type = source_db_info.get('source_db_type')
            res_pk, res_fk = oracle_source.oracle_source_pk_fk(from_tables)
            for from_table in from_tables:
                # oracle源表
                res_tablestatus, res_partitions, res_columns, res_triggers, res_segments = oracle_source.oracle_source_table(from_table)
                index_column_info = oracle_source.oracle_source_index(from_table)
                res_comments = oracle_source.oracle_source_comment(from_table)
                # oracle->mysql的转换
                mysql_convert = MysqlMetadataMapping(source_type, from_table, res_pk)
                mysql_columns = mysql_convert.column_convert(res_columns, res_comments)
                mysql_tablestatus = mysql_convert.table_comment_convert(res_comments)
                mysql_indexes = mysql_convert.index_convert(index_column_info, res_columns)
                # 检查mysql目标表是否存在
                exist_table_list = mysql_target.mysql_target_exist_tables()
                to_table = from_table
                if to_table in exist_table_list and table_exists_action == 'drop':
                    # 删除目标表
                    mysql_target.mysql_target_execute_no_trans(
                        'drop table if exists `' + to_db + '`.`' + to_table + '`')
                    # 创建表
                    mysql_target.mysql_target_table(to_table, table_exists_action, res_columns=mysql_columns,
                                                    res_tablestatus=mysql_tablestatus)
                    # 创建索引
                    mysql_target.mysql_target_index(to_table, mysql_indexes)
                    # oracle->mysql的trigger同步(需要么?)
                    # to_do
                elif to_table in exist_table_list and table_exists_action == 'truncate':
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    sys.exit(1)
                elif to_table in exist_table_list and table_exists_action == 'append':
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    sys.exit(1)
                elif to_table in exist_table_list and table_exists_action == 'skip':
                    continue
                elif to_table not in exist_table_list:
                    # 创建表
                    mysql_target.mysql_target_table(to_table, table_exists_action, res_columns=mysql_columns,
                                                    res_tablestatus=mysql_tablestatus)
                    # 创建索引
                    mysql_target.mysql_target_index(to_table, mysql_indexes)
                    # oracle->mysql的trigger同步(需要么?)
                    # to_do
                else:
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    sys.exit(1)
            # 外键同步
            mysql_fk = MysqlMetadataMapping.fk_convert(res_fk)
            mysql_target.mysql_target_fk(mysql_fk)
            # 同步后启用外键, 在MysqlTarge类关闭时禁用外键
            # mysql_target.mysql_target_execute('set foreign_key_checks=1')

        # 只同步表数据
        elif content == 'data':
            # 同步数据前禁用外键，在MysqlTarget类初始化时禁用外键
            # mysql_target.mysql_target_execute('set foreign_key_checks=0')
            for from_table in from_tables:
                from_table = from_table.lower()
                res_tablestatus, res_partitions, res_columns, res_triggers, res_segments = oracle_source.oracle_source_table(from_table)
                index_column_info = oracle_source.oracle_source_index(from_table)
                # 检查目标表是否已存在
                exist_table_list = mysql_target.mysql_target_exist_tables()
                to_table = from_table
                if to_table in exist_table_list and table_exists_action == 'drop':
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    sys.exit(1)
                elif to_table in exist_table_list and table_exists_action == 'truncate':
                    # 先truncate目标表，然后追加数据
                    mysql_target.mysql_target_execute_no_trans('truncate table `' + to_db + '`.`' + to_table + '`')
                    # 同步数据, 源库是oracle，执行OracleDataMigrate
                    oracle_dbm = OracleDataMigrate(source_db_info, target_db_info)
                    parallel_flag, final_parallel, parallel_key, parallel_method, lob_flag = oracle_dbm.oracle_parallel_flag(
                        from_table, index_column_info, res_columns, res_segments, parallel=parallel)
                    if parallel_flag == 0:
                        total_rows = oracle_dbm.oracle_serial_migrate(from_table, to_table)
                    else:
                        total_rows = oracle_dbm.oracle_parallel_migrate(from_table, to_table, final_parallel,
                                                                        parallel_key=parallel_key,
                                                                        parallel_method=parallel_method,
                                                                        lob_flag=lob_flag)
                    print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                elif to_table in exist_table_list and table_exists_action == 'append':
                    # 数据追加到目标表
                    oracle_dbm = OracleDataMigrate(source_db_info, target_db_info)
                    parallel_flag, final_parallel, parallel_key, parallel_method, lob_flag = oracle_dbm.oracle_parallel_flag(
                        from_table, index_column_info, res_columns, res_segments, parallel=0)
                    if parallel_flag == 0:
                        total_rows = oracle_dbm.oracle_serial_migrate(from_table, to_table)
                    else:
                        total_rows = oracle_dbm.oracle_parallel_migrate(from_table, to_table, final_parallel,
                                                                        parallel_key=parallel_key,
                                                                        parallel_method=parallel_method,
                                                                        lob_flag=lob_flag)
                    print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                elif to_table in exist_table_list and table_exists_action == 'skip':
                    print('[DBM] table ' + to_table + ' skiped due to table_exists_action == skip')
                else:
                    print('[DBM] error 101 : 目标表[%s]不存在' % to_table)
            # 同步后启用外键, 在MysqlTarge类关闭时禁用外键
            # mysql_target.mysql_target_execute('set foreign_key_checks=1')
        # 带where条件的同步
        elif content == 'increment':
            # 同步数据前禁用外键，在MysqlTarget类初始化时禁用外键
            # mysql_target.mysql_target_execute('set foreign_key_checks=0')
            for from_table in from_tables:
                from_table = from_table.lower()
                res_tablestatus, res_partitions, res_columns, res_triggers, res_segments = oracle_source.oracle_source_table(
                    from_table)
                index_column_info = oracle_source.oracle_source_index(from_table)
                # 检查目标表是否已存在
                exist_table_list = mysql_target.mysql_target_exist_tables()
                to_table = from_table
                if to_table in exist_table_list and table_exists_action == 'drop':
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    sys.exit(1)
                elif to_table in exist_table_list and table_exists_action == 'truncate':
                    # 先truncate目标表，然后追加数据
                    mysql_target.mysql_target_execute_no_trans('truncate table `' + to_db + '`.`' + to_table + '`')
                    # 同步数据, 源库是oracle，执行OracleDataMigrate，增量同步目前只支持串行
                    oracle_dbm = OracleDataMigrate(source_db_info, target_db_info)
                    if incremental_method == 'where':
                        if not where_clause:
                            print('[DBM] note 300: 增量同步方式为where，但未指定where_clause，将启用全量同步')
                        elif not (where_clause.startswith('where') or where_clause.startswith('WHERE')):
                            print('[DBM] error 100: 参数错误, where_clause=%s 参数错误，where_clause必须以where开头.' % where_clause)
                            sys.exit(1)
                        total_rows = oracle_dbm.oracle_incr_serial_migrate(from_table, to_table,
                                                                           incremental_method=incremental_method,
                                                                           where_clause=where_clause)
                        print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                    else:
                        print(
                            '[DBM] error 100 : 参数错误，incremental_method=%s 参数错误，目前仅支持where_clause方式.' % incremental_method)
                elif to_table in exist_table_list and table_exists_action == 'append':
                    # 同步数据, 源库是oracle，执行OracleDataMigrate，增量同步目前只支持串行
                    oracle_dbm = OracleDataMigrate(source_db_info, target_db_info)
                    if incremental_method == 'where':
                        if not where_clause:
                            print('[DBM] note 300: 增量同步方式为where，但未指定where_clause，将启用全量同步')
                        elif not (where_clause.startswith('where') or where_clause.startswith('WHERE')):
                            print('[DBM] error 100: 参数错误, where_clause=%s 参数错误，where_clause必须以where开头.' % where_clause)
                            sys.exit(1)
                        total_rows = oracle_dbm.oracle_incr_serial_migrate(from_table, to_table,
                                                                           incremental_method=incremental_method,
                                                                           where_clause=where_clause)
                        print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                    else:
                        print(
                            '[DBM] error 100 : 参数错误，incremental_method=%s 参数错误，目前仅支持where_clause方式.' % incremental_method)
                elif to_table in exist_table_list and table_exists_action == 'skip':
                    print('[DBM] table ' + to_table + ' skiped due to table_exists_action == skip')
                else:
                    print('[DBM] error 101 : 目标表[%s]不存在' % to_table)
            # 同步后启用外键, 在MysqlTarge类关闭时禁用外键
            # mysql_target.mysql_target_execute('set foreign_key_checks=1')
        else:
            print("[DBM] error 999 : 目前Oracle->Mysql的数据库同步只支持表级别的同步!")
            sys.exit(1)
    else:
        print('[DBM] error 100: source_tables=%s 参数错误.' % source_tables)
        sys.exit(1)


# mssql -> oracle 数据库同步
def mssql_to_oracle():
    # 检查源库mssql和目标库oracle, 检查源表是否存在
    mssql_source = MssqlSource(**source_db_info)
    oracle_target = OracleTarget(**target_db_info)
    from_tables, migrate_granularity = mssql_source.source_table_check(*source_tables)
    to_tables = from_tables

    # mssql -> oracle 数据库级别同步
    if migrate_granularity == 'db':
        print("[DBM] error 999 : 目前Mssql->Oracle的数据库同步只支持表级别的数据同步(content='data')!")
        sys.exit(1)
    # mssql -> oracle 表级别同步
    elif migrate_granularity == 'table':
        # 只同步表数据
        if content == 'data':
            # 同步数据前禁用外键
            sql_enable_constraints = oracle_target.oracle_disable_constraint(to_tables)
            for from_table in from_tables:
                from_table = from_table.lower()
                res_tablestatus, res_columns = mssql_source.mssql_source_table(from_table)
                index_column_info = mssql_source.mssql_source_index(from_table)
                # 检查目标表是否已存在
                exist_table_list = oracle_target.oracle_target_exist_tables()
                to_table = from_table
                if to_table in exist_table_list and table_exists_action == 'drop':
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    sys.exit(1)
                elif to_table in exist_table_list and table_exists_action == 'truncate':
                    # 先truncate目标表，然后追加数据
                    truncate_sql = 'truncate table ' + to_table
                    oracle_target.oracle_execute_dml(truncate_sql)
                    # 同步数据, 源库是mssql，执行MssqlDataMigrate
                    mssql_dbm = MssqlDataMigrate(source_db_info, target_db_info)
                    parallel_flag, final_parallel, parallel_key, parallel_method = mssql_dbm.mssql_parallel_flag(
                        from_table, index_column_info, res_tablestatus, res_columns, parallel=parallel)
                    if parallel_flag == 0:
                        total_rows = mssql_dbm.mssql_serial_migrate(from_table, to_table)
                    else:
                        total_rows = mssql_dbm.mssql_parallel_migrate(from_table, to_table, final_parallel,
                                                                      parallel_key=parallel_key,
                                                                      parallel_method=parallel_method)
                    print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                elif to_table in exist_table_list and table_exists_action == 'append':
                    # 数据追加到目标表
                    mssql_dbm = MssqlDataMigrate(source_db_info, target_db_info)
                    parallel_flag, final_parallel, parallel_key, parallel_method = mssql_dbm.mssql_parallel_flag(
                        from_table, index_column_info, res_tablestatus, res_columns, parallel=parallel)
                    if parallel_flag == 0:
                        total_rows = mssql_dbm.mssql_serial_migrate(from_table, to_table)
                    else:
                        total_rows = mssql_dbm.mssql_parallel_migrate(from_table, to_table, final_parallel,
                                                                      parallel_key=parallel_key,
                                                                      parallel_method=parallel_method)
                    print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                elif to_table in exist_table_list and table_exists_action == 'skip':
                    print('[DBM] table ' + to_table + ' skiped due to table_exists_action == skip')
                else:
                    print('[DBM] error 101 : 目标表[%s]不存在' % to_table)
            # 同步后启用外键
            oracle_target.oracle_enable_constraint(sql_enable_constraints)
        # 带where条件的同步
        elif content == 'increment':
            # 同步数据前禁用外键
            sql_enable_constraints = oracle_target.oracle_disable_constraint(to_tables)
            for from_table in from_tables:
                from_table = from_table.lower()
                res_tablestatus, res_columns = mssql_source.mssql_source_table(from_table)
                index_column_info = mssql_source.mssql_source_index(from_table)
                # 检查目标表是否已存在
                exist_table_list = oracle_target.oracle_target_exist_tables()
                to_table = from_table
                if to_table in exist_table_list and table_exists_action == 'drop':
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    sys.exit(1)
                elif to_table in exist_table_list and table_exists_action == 'truncate':
                    # 先truncate目标表，然后追加数据
                    truncate_sql = 'truncate table ' + to_table
                    oracle_target.oracle_execute_dml(truncate_sql)
                    # 同步数据, 源库是mssql，执行MssqlDataMigrate，增量同步目前只支持串行
                    mssql_dbm = MssqlDataMigrate(source_db_info, target_db_info)
                    if incremental_method == 'where':
                        if not where_clause:
                            print('[DBM] note 300: 增量同步方式为where，但未指定where_clause，将启用全量同步')
                        elif not (where_clause.startswith('where') or where_clause.startswith('WHERE')):
                            print('[DBM] error 100: 参数错误, where_clause=%s 参数错误，where_clause必须以where开头.' % where_clause)
                            sys.exit(1)
                        total_rows = mssql_dbm.mssql_incr_serial_migrate(from_table, to_table,
                                                                         incremental_method=incremental_method,
                                                                         where_clause=where_clause)
                        print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                    else:
                        print('[DBM] error 100 : 参数错误，incremental_method=%s 参数错误，目前仅支持where_clause方式.' % incremental_method)
                    mssql_dbm.close()
                elif to_table in exist_table_list and table_exists_action == 'append':
                    # 同步数据, 源库是mssql，执行MssqlDataMigrate，增量同步目前只支持串行
                    mssql_dbm = MssqlDataMigrate(source_db_info, target_db_info)
                    if incremental_method == 'where':
                        if not where_clause:
                            print('[DBM] note 300: 增量同步方式为where，但未指定where_clause，将启用全量同步')
                        elif not (where_clause.startswith('where') or where_clause.startswith('WHERE')):
                            print('[DBM] error 100: 参数错误, where_clause=%s 参数错误，where_clause必须以where开头.' % where_clause)
                            sys.exit(1)
                        total_rows = mssql_dbm.mssql_incr_serial_migrate(from_table, to_table,
                                                                         incremental_method=incremental_method,
                                                                         where_clause=where_clause)
                        print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                    else:
                        print(
                            '[DBM] error 100 : 参数错误，incremental_method=%s 参数错误，目前仅支持where_clause方式.' % incremental_method)
                    mssql_dbm.close()
                elif to_table in exist_table_list and table_exists_action == 'skip':
                    print('[DBM] table ' + to_table + ' skiped due to table_exists_action == skip')
                else:
                    print('[DBM] error 101 : 目标表[%s]不存在' % to_table)
            # 同步后启用外键
            oracle_target.oracle_enable_constraint(sql_enable_constraints)
        else:
            print("[DBM] error 999 : 目前Mssql->Oracle的数据库同步只支持表级别的数据同步(content='data' or content='increment')!")
            sys.exit(1)
    else:
        print('[DBM] error 100: source_tables=%s 参数错误.' % source_tables)
        sys.exit(1)


# mssql -> mysql 数据库同步
def mssql_to_mysql():
    # 检查源库mssql和目标库mysql, 检查源表是否存在
    mssql_source = MssqlSource(**source_db_info)
    mysql_target = MysqlTarget(**target_db_info)
    from_tables, migrate_granularity = mssql_source.source_table_check(*source_tables)
    to_tables = from_tables
    # mssql -> mysql 数据库级别同步
    if migrate_granularity == 'db':
        print("[DBM] error 999 : 目前Mssql->Mysql的数据库同步只支持表级别的同步!")
        sys.exit(1)
    elif migrate_granularity == 'table':
        # mssql->mysql表级别数据同步
        if content == 'data':
            # 同步数据前禁用外键，在MysqlTarget类初始化时禁用外键
            # mysql_target.mysql_target_execute('set foreign_key_checks=0')
            for from_table in from_tables:
                from_table = from_table.lower()
                res_tablestatus, res_columns = mssql_source.mssql_source_table(from_table)
                index_column_info = mssql_source.mssql_source_index(from_table)
                # 检查目标表是否已存在
                exist_table_list = mysql_target.mysql_target_exist_tables()
                to_table = from_table
                if to_table in exist_table_list and table_exists_action == 'drop':
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    sys.exit(1)
                elif to_table in exist_table_list and table_exists_action == 'truncate':
                    # 先truncate目标表，然后追加数据
                    mysql_target.mysql_target_execute_no_trans('truncate table `' + to_db + '`.`' + to_table + '`')
                    # 同步数据, 源库是mssql，执行MssqlDataMigrate
                    mssql_dbm = MssqlDataMigrate(source_db_info, target_db_info)
                    parallel_flag, final_parallel, parallel_key, parallel_method = mssql_dbm.mssql_parallel_flag(
                        from_table, index_column_info, res_tablestatus, res_columns, parallel=parallel)
                    if parallel_flag == 0:
                        total_rows = mssql_dbm.mssql_serial_migrate(from_table, to_table)
                    else:
                        total_rows = mssql_dbm.mssql_parallel_migrate(from_table, to_table, final_parallel,
                                                                      parallel_key=parallel_key,
                                                                      parallel_method=parallel_method)
                    print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                elif to_table in exist_table_list and table_exists_action == 'append':
                    # 数据追加到目标表
                    mssql_dbm = MssqlDataMigrate(source_db_info, target_db_info)
                    parallel_flag, final_parallel, parallel_key, parallel_method = mssql_dbm.mssql_parallel_flag(
                        from_table, index_column_info, res_tablestatus, res_columns, parallel=parallel)
                    if parallel_flag == 0:
                        total_rows = mssql_dbm.mssql_serial_migrate(from_table, to_table)
                    else:
                        total_rows = mssql_dbm.mssql_parallel_migrate(from_table, to_table, final_parallel,
                                                                      parallel_key=parallel_key,
                                                                      parallel_method=parallel_method)
                    print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                elif to_table in exist_table_list and table_exists_action == 'skip':
                    print('[DBM] table ' + to_table + ' skiped due to table_exists_action == skip')
                else:
                    print('[DBM] error 101 : 目标表[%s]不存在' % to_table)
            # 同步后启用外键, 在MysqlTarge类关闭时禁用外键
            # mysql_target.mysql_target_execute('set foreign_key_checks=1')
        # 带where条件的同步
        elif content == 'increment':
            # 同步数据前禁用外键，在MysqlTarget类初始化时禁用外键
            # mysql_target.mysql_target_execute('set foreign_key_checks=0')
            for from_table in from_tables:
                from_table = from_table.lower()
                res_tablestatus, res_columns = mssql_source.mssql_source_table(from_table)
                index_column_info = mssql_source.mssql_source_index(from_table)
                # 检查目标表是否已存在
                exist_table_list = mysql_target.mysql_target_exist_tables()
                to_table = from_table
                if to_table in exist_table_list and table_exists_action == 'drop':
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    sys.exit(1)
                elif to_table in exist_table_list and table_exists_action == 'truncate':
                    # 先truncate目标表，然后追加数据
                    mysql_target.mysql_target_execute_no_trans('truncate table `' + to_db + '`.`' + to_table + '`')
                    # 同步数据, 源库是mssql，执行MssqlDataMigrate，增量同步目前只支持串行
                    mssql_dbm = MssqlDataMigrate(source_db_info, target_db_info)
                    if incremental_method == 'where':
                        if not where_clause:
                            print('[DBM] note 300: 增量同步方式为where，但未指定where_clause，将启用全量同步')
                        elif not (where_clause.startswith('where') or where_clause.startswith('WHERE')):
                            print('[DBM] error 100: 参数错误, where_clause=%s 参数错误，where_clause必须以where开头.' % where_clause)
                            sys.exit(1)
                        total_rows = mssql_dbm.mssql_incr_serial_migrate(from_table, to_table,
                                                                         incremental_method=incremental_method,
                                                                         where_clause=where_clause)
                        print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                    else:
                        print('[DBM] error 100 : 参数错误，incremental_method=%s 参数错误，目前仅支持where_clause方式.' % incremental_method)
                    mssql_dbm.close()
                elif to_table in exist_table_list and table_exists_action == 'append':
                    # 同步数据, 源库是mssql，执行MssqlDataMigrate，增量同步目前只支持串行
                    mssql_dbm = MssqlDataMigrate(source_db_info, target_db_info)
                    if incremental_method == 'where':
                        if not where_clause:
                            print('[DBM] note 300: 增量同步方式为where，但未指定where_clause，将启用全量同步')
                        elif not (where_clause.startswith('where') or where_clause.startswith('WHERE')):
                            print('[DBM] error 100: 参数错误, where_clause=%s 参数错误，where_clause必须以where开头.' % where_clause)
                            sys.exit(1)
                        total_rows = mssql_dbm.mssql_incr_serial_migrate(from_table, to_table,
                                                                         incremental_method=incremental_method,
                                                                         where_clause=where_clause)
                        print('[DBM] Inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                    else:
                        print('[DBM] error 100 : 参数错误，incremental_method=%s 参数错误，目前仅支持where_clause方式.' % incremental_method)
                    mssql_dbm.close()
                elif to_table in exist_table_list and table_exists_action == 'skip':
                    print('[DBM] table ' + to_table + ' skiped due to table_exists_action == skip')
                else:
                    print('[DBM] error 101 : 目标表[%s]不存在' % to_table)
            # 同步后启用外键, 在MysqlTarge类关闭时禁用外键
            # mysql_target.mysql_target_execute('set foreign_key_checks=1')
        else:
            print("[DBM] error 999 : 目前Mssql->Mysql的数据库同步只支持表级别的同步!")
            sys.exit(1)
    else:
        print('[DBM] error 100: source_tables=%s 参数错误.' % source_tables)
        sys.exit(1)


# 版本控制
def dbm_version(current_release=None):
    if current_release:
        release_url = 'http://172.20.222.218:9003/dbm_check_version?current_release=' + str(current_release)
        update1_url = 'http://172.20.222.218:9003/dbm_latest_version/dbmigrate'
        update2_url = 'http://172.20.222.218:9003/dbm_latest_version/dbmigrate.conf'
        try:
            resp = requests.get(release_url)
            if resp.status_code == 444:
                print('dbmigrate版本更新中，功能和性能进一步增强~~~~')
                resp1 = requests.get(update1_url)
                with open("dbmigrate_new", 'wb') as f:
                    f.write(resp1.content)
                os.chmod("dbmigrate_new", stat.S_IRWXU|stat.S_IRWXG)
                os.rename("dbmigrate", "dbmigrate_old")
                os.rename("dbmigrate_new", "dbmigrate")
                resp2 = requests.get(update2_url)
                with open("dbmigrate.conf.sample", 'wb') as f:
                    f.write(resp2.content)
                print('dbmigrate版本更新完成，请重新启动.')
                os._exit(0)
        except:
            pass


# 主程序
if __name__ == '__main__':
    # 增加版本标识
    current_release = 20201203
    print('dbmigrate version: ' + str(current_release))
    if not (auto_upgrade and (auto_upgrade == 0 or auto_upgrade == '0')):
        dbm_version(current_release=current_release)

    # 参数
    begin_time = time.time()
    current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(current_time + ' DBM开始同步: source_db:' + source_host + ':' + str(
        source_port) + '/' + source_db + ' => ' + 'target_db:' + target_host + ':' + str(target_port) + '/' + target_db)
    source_db_type = source_db_type
    target_db_type = target_db_type
    target_db = target_db if target_db else source_db
    to_db = target_db
    # source_tables = list(map(lambda x: x.lower(), source_tables))
    content = content
    source_db_info = {'host': source_host, 'port': source_port, 'db': source_db, 'user': source_user,
                      'password': source_password, 'charset': 'utf8', 'source_db_type': source_db_type}
    target_db_info = {'host': target_host, 'port': target_port, 'db': target_db, 'user': target_user,
                      'password': target_password, 'charset': 'utf8', 'target_db_type': target_db_type}

    # 初步判断同步粒度
    if source_tables:
        migrate_granularity = 'table'
    else:
        migrate_granularity = 'db'

    # 开始同步
    if source_db_type == 'mysql' and target_db_type == 'mysql':
        mysql_to_mysql()
    elif source_db_type == 'mysql' and target_db_type == 'oracle':
        mysql_to_oracle()
    elif source_db_type == 'oracle' and target_db_type == 'oracle':
        oracle_to_oracle()
    elif source_db_type == 'oracle' and target_db_type == 'mysql':
        oracle_to_mysql()
    elif source_db_type == 'mssql' and target_db_type == 'oracle':
        mssql_to_oracle()
    elif source_db_type == 'mssql' and target_db_type == 'mysql':
        mssql_to_mysql()
    end_time = time.time()
    current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(current_time + ' DBM同步完成,共耗时:' + str(round(end_time - begin_time)) + 's')












