# -*- code: utf-8 -*-

'''
数据库迁移主程序
'''
import traceback
import time, sys
from env import *
from mysql_migrate import MysqlSource, MysqlTarget, MysqlDataMigrate
from oracle_migrate import OracleTarget


# mysql -> mysql 数据库同步
def mysql_to_mysql():
    # mysql检查源库和源表,检查目标库
    mysql_source = MysqlSource(**source_db_info)
    mysql_target = MysqlTarget(**target_db_info)
    mysql_source.source_db_check()
    from_tables, migrate_granularity = mysql_source.source_table_check(*source_tables)
    mysql_target.mysql_target_createdb(migrate_granularity)
    target_tables = from_tables
    mysql_target.mysql_target_execute('set foreign_key_checks=0')

    # mysql -> mysql 数据库级别同步
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

        elif content == 'data':
            print('[DBM] error 100 : 参数错误，content=\'data\' 仅适用于表同步.')
            sys.exit(1)
        else:
            print('[DBM] error 100 : 参数错误，content=%s.' % content)
            sys.exit(1)
    # mysql -> mysql 表级别同步
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
                    mysql_target.msyql_target_index(to_table, index_column_info)

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
                    mysql_target.msyql_target_index(to_table, index_column_info)

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
    else:
        print('[DBM] error 100: source_tables=%s 参数错误.' % source_tables)
        sys.exit(1)
    #恢复约束
    mysql_target.mysql_target_execute('set foreign_key_checks=1')


# mysql -> oracle 数据库同步
def mysql_to_oracle():
    # 检查源库mysql和目标库oracle, 检查源表是否存在
    mysql_source = MysqlSource(**source_db_info)
    oracle_target = OracleTarget(**target_db_info)
    mysql_source.source_db_check()
    from_tables, migrate_granularity = mysql_source.source_table_check(*source_tables)
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
                    print('[DBM] inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
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
                    print('[DBM] inserted ' + str(total_rows) + ' rows into table `' + to_table + '`')
                elif to_table in exist_table_list and table_exists_action == 'skip':
                    print('[DBM] table ' + to_table + 'skiped due to table_exists_action == skip')
                else:
                    print('[DBM] error 101 : 目标表[%s]不存在' % to_table)
        else:
            print("[DBM] error 999 : 目前Mysql->Oracle的异构数据库同步只支持表级别的数据同步(content='data')!")
            sys.exit(1)
    else:
        print('[DBM] error 100: source_tables=%s 参数错误.' % source_tables)
        sys.exit(1)



# 主程序
if __name__ == '__main__':
    # 参数
    begin_time = time.time()
    print('DBM开始同步: source_db:' + source_host + ':' + str(
        source_port) + '/' + source_db + ' => ' + 'target_db:' + target_host + ':' + str(target_port) + '/' + target_db)
    source_db_type = source_db_type
    target_db_type = target_db_type
    target_db = target_db if target_db else source_db
    to_db = target_db
    source_db_info = {'host': source_host, 'port': source_port, 'db': source_db, 'user': source_user,
                      'password': source_password, 'charset': 'utf8', 'source_db_type': source_db_type}
    target_db_info = {'host': target_host, 'port': target_port, 'db': target_db, 'user': target_user,
                      'password': target_password, 'charset': 'utf8', 'target_db_type': target_db_type}

    #判断同步粒度
    if source_tables:
        migrate_granularity = 'table'
    else:
        migrate_granularity = 'db'

    # 开始同步
    if source_db_type == 'mysql' and target_db_type == 'mysql':
        mysql_to_mysql()
    elif source_db_type == 'mysql' and target_db_type == 'oracle':
        mysql_to_oracle()
    end_time = time.time()
    print('DBM同步完成,共耗时:' + str(round(end_time - begin_time)) + 's')












