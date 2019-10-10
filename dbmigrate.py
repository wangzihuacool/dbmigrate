# -*- code: utf-8 -*-

'''
数据库迁移主程序
'''
import traceback
import time
from env import *
from mysql_migrate import MysqlSource, MysqlTarget, MysqlDataMigrate



if __name__ == '__main__':
    #连接mysql源库和目标库
    begin_time = time.time()
    target_db = target_db if target_db else source_db
    to_db = target_db
    source_db_info = {'host': source_host, 'port': source_port, 'db': source_db, 'user': source_user,
                      'password': source_password, 'charset': 'utf8'}
    target_db_info = {'host': target_host, 'port': target_port, 'db': target_db, 'user': target_user,
                      'password': target_password, 'charset': 'utf8'}
    mysql_source = MysqlSource(**source_db_info)
    mysql_target = MysqlTarget(**target_db_info)

    #检查源库和源表,检查目标库
    mysql_source.source_db_check()
    from_tables, migrate_granularity = mysql_source.source_table_check(*source_tables)
    mysql_target.mysql_target_createdb(migrate_granularity)
    target_tables = from_tables
    mysql_target.mysql_target_execute('set foreign_key_checks=0')

    #migrate
    #全库同步
    print('DBM开始同步: source_db:' + source_host + ':' + str(source_port) + '/' + source_db + ' => ' + 'target_db:' + target_host + ':' + str(target_port) + '/' + target_db )
    if migrate_granularity == 'db':
        # 同步全库所有元数据+数据
        if content == 'all':
            #表同步
            for from_table in from_tables:
                res_tablestatus, res_createtable, res_columns, res_triggers = mysql_source.mysql_source_table(from_table)
                index_column_info = mysql_source.mysql_source_index(from_table)
                # 目标表
                exist_table_list = mysql_target.mysql_target_exist_tables()
                to_table = from_table
                #标准处理
                #创建表
                mysql_target.mysql_target_table(to_table, table_exists_action, res_columns=res_columns,
                                                res_tablestatus=res_tablestatus)
                #创建索引
                mysql_target.msyql_target_index(to_table, index_column_info)
                #同步数据
                mysql_dbm = MysqlDataMigrate(source_db_info, target_db_info)
                parallel_flag, final_parallel, parallel_key, parallel_method = mysql_dbm.mysql_parallel_flag(from_table, res_tablestatus, res_columns, parallel=parallel)
                if parallel_flag == 0:
                    total_rows = mysql_dbm.mysql_serial_migrate(from_table, to_table)
                else:
                    total_rows = mysql_dbm.mysql_parallel_migrate(from_table, to_table, final_parallel, parallel_key=parallel_key, parallel_method=parallel_method)
                print('[DBM] inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')

                # trigger同步
                # to_do
            #外键同步
            final_fk = mysql_source.mysql_source_fk(from_tables)
            mysql_target.mysql_target_fk(final_fk)

            #视图、存储过程，函数和routines同步
            #to_db

        elif content == 'metadata':
            # 表同步
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
                mysql_target.msyql_target_index(to_table, index_column_info)

                # trigger同步
                # to_do
            # 外键同步
            final_fk = mysql_source.mysql_source_fk(from_tables)
            mysql_target.mysql_target_fk(final_fk)

            # 视图、存储过程，函数和routines同步
            # to_db

        elif content == 'data':
            print('[DBM] error 100 : 参数错误，content=\'data\' 仅适用于表同步.')
            exit(1)
        else:
            print('[DBM] error 100 : 参数错误，content=%s.' % content)
            exit(1)
    #指定表同步
    elif migrate_granularity == 'table':
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
                    mysql_target.msyql_target_index(to_table, index_column_info)
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
                    print('[DBM] inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')

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
                    print('[DBM] inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')

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
                    print('[DBM] inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')

                elif to_table in exist_table_list and table_exists_action == 'skip':
                    continue

                elif to_table not in exist_table_list:
                    # 表在目标库不存在，标准处理
                    # 创建表
                    mysql_target.mysql_target_table(to_table, table_exists_action, res_columns=res_columns,
                                                    res_tablestatus=res_tablestatus)
                    # 创建索引
                    mysql_target.msyql_target_index(to_table, index_column_info)
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
                    print('[DBM] inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')

                    # trigger同步
                    # to_do
                else:
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    exit(1)

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
                    mysql_target.msyql_target_index(to_table, index_column_info)

                    # trigger同步
                    # to_do
                elif to_table in exist_table_list and table_exists_action == 'truncate':
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    exit(1)
                elif to_table in exist_table_list and table_exists_action == 'append':
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    exit(1)
                elif to_table in exist_table_list and table_exists_action == 'skip':
                    continue
                elif to_table not in exist_table_list:
                    # 创建表
                    mysql_target.mysql_target_table(to_table, table_exists_action, res_columns=res_columns,
                                                    res_tablestatus=res_tablestatus)
                    # 创建索引
                    mysql_target.msyql_target_index(to_table, index_column_info)

                    # trigger同步
                    # to_do
                else:
                    print('[DBM] error 100 : 参数错误，table_exists_action=%s 参数错误.' % table_exists_action)
                    exit(1)
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
                    exit(1)
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
                    print('[DBM] inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
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
                    print('[DBM] inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                elif to_table in exist_table_list and table_exists_action == 'skip':
                    print('[DBM] table ' + to_table + 'skiped due to table_exists_action == skip')
                else:
                    print('[DBM] error 101 : 目标表[%s]不存在' % to_table)
        else:
            print('[DBM] error 100 : content=%s 参数错误.' % content)
    #恢复约束
    mysql_target.mysql_target_execute('set foreign_key_checks=1')
    end_time = time.time()
    print('DBM同步完成,共耗时:' + str(end_time - begin_time) + 's')











