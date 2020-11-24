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
from mysql_operate import MysqlOperate, MysqlOperateIncr
from oracle_migrate import OracleTarget
from comm_decorator import performance, MyThread, escape_string
import pysnooper
#from itertools import groupby



# 源库为mysql时获取数据
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
            sys.exit(1)

    # 检查源库是否存在
    def source_db_check(self):
        res_db = self.MysqlDb.mysql_select('show databases')
        if self.from_db in [db[0] for db in res_db]:
            pass
        else:
            print('[DBM] Error: Source db [' + self.from_db + '] not found.请确认!')
            sys.exit(1)

    # 检查配置文件中的表在源库是否存在，未配置则全部表同步
    def source_table_check(self, *source_tables, **content):
        res_tables = self.MysqlDb.mysql_select('show full tables from `%s` where table_type != "VIEW"' % self.from_db)
        # 检查源库是否大小写敏感,如果大小写不敏感则全部转换为小写. added by wl_lw at 20200612
        res_case_sensitive = self.MysqlDb.mysql_select("show variables like 'lower_case_table_names'")
        if res_case_sensitive[0][0] == 1:
            source_tables = list(map(lambda x: x.lower(), source_tables))
            all_table_list = [table[0].lower() for table in res_tables]
        else:
            source_tables = source_tables
            all_table_list = [table[0] for table in res_tables]
        content = content.get('content')
        if source_tables:
            if set(all_table_list) >= set(source_tables):
                from_tables = source_tables
                migrate_granularity = 'table'
            else:
                not_exists_tables = set(source_tables) - set(all_table_list)
                print('[DBM] Error: 源数据库[' + self.from_db + ']中不存在表:' + str(not_exists_tables) + '.请确认!')
                sys.exit(1)
        elif not source_tables and (content == 'all' or content == 'metadata'):
            from_tables = all_table_list
            migrate_granularity = 'db'
        elif not source_tables and content == 'data':
            print('[DBM] error 100 : 参数错误，content=\'data\' 仅适用于表同步.')
            sys.exit(1)
        elif not source_tables and content == 'index':
            from_tables = all_table_list
            migrate_granularity = 'table'
        else:
            print('[DBM] Error: source_tables 和 content参数错误.请确认!')
            sys.exit(1)
        return from_tables, migrate_granularity

    # 获取源表元数据
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

    # 获取源表索引(除去主键,主键跟表元数据一起)
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

    # 全量获取源表数据
    def mysql_source_data(self, sql):
        # 设置session级SQL最大执行时间为1小时
        sql_max_exec_time = 'set max_execution_time=3600000'
        self.MysqlDb.mysql_execute(sql_max_exec_time)
        res_data = self.MysqlDb.mysql_select(sql)
        return res_data

    # 获取源表列名，用于源库和目标库的表结构不完全一致的情况 added by wl_lw at 20200618
    def mysql_source_column(self, sql):
        columns = self.MysqlDb.mysql_columns(sql)
        return columns

    # 获取源表涉及的外键
    def mysql_source_fk(self, from_tables):
        res_fk = self.MysqlDb.execute_dict(
            'SELECT distinct c.constraint_name, c.table_schema, c.table_name, c.column_name, c.ordinal_position, c.position_in_unique_constraint, c.referenced_table_schema, c.referenced_table_name, c.referenced_column_name, r.update_rule, r.delete_rule from information_schema.key_column_usage c	join information_schema.referential_constraints r on c.table_name = r.table_name and c.constraint_name = r.constraint_name and c.referenced_table_name = r.referenced_table_name WHERE c.table_schema = "%s" AND c.constraint_name NOT IN ( "PRIMARY", "primary" ) and c.referenced_table_name is not null' % self.from_db)
        # 排序参考 https://docs.python.org/3/howto/sorting.html
        res_fk.sort(key=itemgetter('constraint_name', 'ordinal_position', 'position_in_unique_constraint'))
        # print(res_fk)
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
        #print(final_fk)
        return final_fk

    #获取源库的所有视图,存储过程，函数和routines
    def mysql_source_pkg(self):
        # 获取源库的视图及其定义
        res_views = self.MysqlDb.mysql_select('select table_name, check_option, is_updatable, security_type, '
                                              'definer from information_schema.views where table_schema="%s" '
                                              'order by table_name asc' % self.from_db)
        from_views = [view[0] for view in res_views]
        from_views_tmp_ddl = {}
        from_views_ddl = {}
        if from_views:
            for from_view in from_views:
                res_view = self.MysqlDb.mysql_select('show create view `%s`.`%s`' % (self.from_db, from_view))
                view_tmp_defination = self.MysqlDb.mysql_select('show fields from `%s`.`%s`' % (self.from_db, from_view))
                from_views_tmp_ddl.setdefault(from_view, view_tmp_defination)
                from_views_ddl.setdefault(from_view, res_view[0])

        # 获取源库的存储过程及其定义
        res_procedures = self.MysqlDb.mysql_select('show procedure status where db = "%s"' % self.from_db)
        from_procedures = [procedure[1] for procedure in res_procedures]
        from_procedures_ddl = {}
        if from_procedures:
            for from_procedure in from_procedures:
                res_procedure = self.MysqlDb.mysql_select('show create procedure `%s`.`%s`' % (self.from_db, from_procedure))
                from_procedures_ddl.setdefault(from_procedure, res_procedure[0])

        # 获取源库的函数及其定义
        res_functions = self.MysqlDb.mysql_select('show function status where db = "%s"' % self.from_db)
        from_functions = [f[1] for f in res_functions]
        from_functions_ddl = {}
        if from_functions:
            for from_function in from_functions:
                res_function = self.MysqlDb.mysql_select('show create function `%s`.`%s`' % (self.from_db, from_function))
                from_functions_ddl.setdefault(from_function, res_function[0])

        # routines(函数+存储过程) 和 event
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
        return from_views_ddl, from_views_tmp_ddl, from_procedures_ddl, from_functions_ddl, from_routines, from_events

    def close(self):
        self.MysqlDb.close()


# 目标为mysql时写入数据
class MysqlTarget(object):
    # 建立目标库连接,模式关闭外键，退出时开启外键
    def __init__(self, **target_db_info):
        # self.new_db_info = copy.deepcopy(target_db_info)
        # new_db_info['db'] = None
        try:
            self.MysqlTargetDb = MysqlOperate(**target_db_info)
            self.to_db = target_db_info.get('db')
            self.MysqlTargetDb.mysql_execute('use %s' % self.to_db)
            self.MysqlTargetDb.mysql_execute_no_trans('set foreign_key_checks=0')
        except Exception as e:
            print('DBM Error: can not connect to target db: ' + target_db_info.get('host') + ':' +
                  str(target_db_info.get('port')) + '/' + target_db_info.get('db'))
            traceback.print_exc()
            sys.exit(1)

    # 创建目标数据库, 这里单独建立连接，db设置为None
    @staticmethod
    def mysql_target_createdb(migrate_granularity, **target_db_info):
        new_db_info = copy.deepcopy(target_db_info)
        new_db_info['db'] = None
        try:
            MysqlTargetDb = MysqlOperate(**new_db_info)
            to_db = target_db_info.get('db')
        except Exception as e:
            print('DBM Error: can not connect to target db: ' + target_db_info.get('host') + ':' +
                  str(target_db_info.get('port')) + '/' + target_db_info.get('db'))
            traceback.print_exc()
            sys.exit(1)
        res_db = MysqlTargetDb.mysql_select('show databases')
        if to_db in [db[0] for db in res_db] and migrate_granularity == 'db':
            # print('DBM Warnning: 数据库级别的同步会删除目标库，请确认！')
            var = input('DBM Warnning: 数据库级别的同步会删除目标库,继续?y[n]')
            if var != 'y' and var != 'Y':
                sys.exit(1)
            MysqlTargetDb.mysql_execute('drop database if exists %s' % to_db)
            MysqlTargetDb.mysql_execute('create database if not exists %s' % to_db)
        elif to_db in [db[0] for db in res_db] and migrate_granularity == 'table':
            pass
        else:
            MysqlTargetDb.mysql_execute('create database if not exists %s' % to_db)

    # 检查目标库已存在的表
    def mysql_target_exist_tables(self):
        res_tables = self.MysqlTargetDb.mysql_select('show full tables from `%s` where table_type != "VIEW"' % self.to_db)
        all_table_list = [table[0] for table in res_tables]
        return all_table_list

    # 目标库创建表
    def mysql_target_table(self, to_table, table_exists_action, res_columns=None, res_tablestatus=None):
        self.MysqlTargetDb.mysql_execute('use %s' % self.to_db)
        #处理列信息
        all_columns_defination = ''
        for res_row in res_columns:
            field_defination = '`' + res_row.get('field') + '`'
            type_defination = res_row.get('type')
            charset_defination = ('character set ' + res_row.get('collation').split('_')[0]) if res_row.get('collation') else ''
            collation_defination = ('collate ' + res_row.get('collation')) if res_row.get('collation') else ''
            null_defination = 'not null' if res_row.get('null') == 'NO' else 'null'
            char_types = 'char, varchar, text, enum'
            if res_row.get('default') and (res_row.get('type').lower().startswith('char') or res_row.get('type').lower().startswith('varchar') or res_row.get('type').lower().startswith('text') or res_row.get('type').lower().startswith('enum')):
                value_default = escape_string(res_row.get('default'))
                default_defination = 'default "' + value_default + '"'
            elif res_row.get('default') and not (res_row.get('type').lower().startswith('char') or res_row.get('type').lower().startswith('varchar') or res_row.get('type').lower().startswith('text') or res_row.get('type').lower().startswith('enum')):
                value_default = escape_string(res_row.get('default'))
                default_defination = 'default ' + res_row.get('default') if not value_default.startswith(
                    '0000') else 'default "' + res_row.get('default') + '"'
            elif res_row.get('default') == '':
                default_defination = 'default ""'
            elif not res_row.get('default') and res_row.get('null') == 'YES':
                default_defination = 'default null'
            else:
                default_defination = ''
            # mysql 8.0之后针对default值的列，show columns会在extra中生成DEFAULT_GENERATED关键字导致后续创建表报错，替换DEFAULT_GENERATED关键字，added by wl_lw at 20200704
            extra_defination = res_row.get('extra').replace('DEFAULT_GENERATED', '') if res_row.get('extra') else ''
            # 增加comment内容转义，消除其中的特殊字符的影响 modified at 202001007
            # comment_defination = ('comment "' + res_row.get('comment') + '"') if res_row.get('comment') else ''
            value = escape_string(res_row.get('comment'))
            comment_defination = ('comment "' + value + '"') if value else ''
            column_defination = field_defination + ' ' + type_defination + ' ' + charset_defination + ' ' + \
                                collation_defination + ' ' + null_defination + ' ' + default_defination + ' ' + \
                                extra_defination + ' ' + comment_defination + ','
            all_columns_defination = all_columns_defination + column_defination
        all_columns_defination_1 = all_columns_defination.strip(",")

        # 处理主键信息
        pri_key_col = [('`' + row.get('field') + '`') for row in res_columns if row.get('key') == 'PRI' or row.get('key') == 'pri']
        pri_key_defination = (' primary key (' + ", ".join(pri_key_col) + ')') if pri_key_col else ''

        # 处理表默认属性
        engine_defination = 'engine=' + res_tablestatus[0].get('engine', 'InnoDB')
        default_charset_defination = ('default charset=' + res_tablestatus[0].get('collation').split('_')[0]) if res_tablestatus[0].get('collation') else ' '
        default_collation_defination = ('collate=' + res_tablestatus[0].get('collation')) if res_tablestatus[0].get('collation') else ' '
        create_options_defination = res_tablestatus[0].get('create_options') if res_tablestatus[0].get('create_options') else ' '
        default_comment_defination = ('comment="' + res_tablestatus[0].get('comment') + '"') if res_tablestatus[0].get('comment') else ' '
        all_default_defination = engine_defination + ' ' + default_charset_defination + ' ' + \
                                 default_collation_defination + ' ' + create_options_defination + ' ' + default_comment_defination

        # 创建目标表
        if pri_key_defination:
            sql_table_defination = 'create table `' + self.to_db + '`.`' + to_table + '` (' + all_columns_defination + pri_key_defination + ')' + all_default_defination
        else:
            sql_table_defination = 'create table `' + self.to_db + '`.`' + to_table + '` (' + all_columns_defination_1 + ')' + all_default_defination
        # print(sql_table_defination)
        print('[DBM] Create table `' + to_table + '`')
        table_rows = self.MysqlTargetDb.mysql_execute(sql_table_defination)

    # 创建索引(不包含主键)
    # @performance
    def mysql_target_index(self, to_table, index_column_info):
        print('[DBM] Create index on table `' + to_table + '`')
        all_indexes_defination = []
        for j in index_column_info:
            if j.get('non_unique') == 0:
                sql_index = 'alter table `' + self.to_db + '`.`' + j.get('table') + '` add unique index `' + j.get('key_name') + '`(' + j.get('column_name') + ')'
            else:
                sql_index = 'alter table `' + self.to_db + '`.`' + j.get('table') + '` add index ' + j.get('key_name') + '(' + j.get('column_name') + ')'
            all_indexes_defination.append(sql_index)
            index_rows = self.MysqlTargetDb.mysql_execute(sql_index)
        # print(all_indexes_defination)

    # 删除目标表上的非主键索引, 为满足仅索引同步的需求
    def mysql_drop_index(self, to_table):
        print('[DBM] Drop index from table `' + to_table + '`')
        res_indexes = self.MysqlTargetDb.execute_dict('show index from `%s`.`%s`' % (self.to_db, to_table))
        res_indexes.sort(key=itemgetter('key_name', 'seq_in_index'))
        index_set = set(list(filter(lambda x: x != 'primary' and x != 'PRIMARY', map(lambda x: x.get('key_name'), res_indexes))))
        for key_name in index_set:
            sql_drop_index = 'alter table `' + self.to_db + '`.`' + to_table + '` drop index `' + key_name + '`'
            index_rows = self.MysqlTargetDb.mysql_execute(sql_drop_index)

    # 创建外键
    def mysql_target_fk(self, final_fk):
        for final_fk_record in final_fk:
            # 外键对应表所在schema为to_db
            sql_fk = 'alter table `' + self.to_db + '`.`' + final_fk_record.get('table_name') + '` add constraint `' + \
                     final_fk_record.get('constraint_name') + '` foreign key (`' + final_fk_record.get('column_name') + \
                     '`) references `' + self.to_db + '`.`' + final_fk_record.get('referenced_table_name') + \
                     '`(`' + final_fk_record.get('referenced_column_name') + '`) on delete ' + \
                     final_fk_record.get('delete_rule') + ' on update ' + final_fk_record.get('update_rule')
            #print(sql_fk)
            fk_rows = self.MysqlTargetDb.mysql_execute(sql_fk)

    # 创建视图
    def mysql_target_view(self, to_views_ddl):
        for to_view, to_view_ddl in to_views_ddl.items():
            print('[DBM] Create view `' + to_view + '`')
            sql_drop_view = 'drop table if exists `' + self.to_db + '`.`' + to_view + '`'
            sql_view = 'create view `' + self.to_db + '`.`' + to_view + '` as ' + to_view_ddl[1].split('AS', 1)[1]
            view_drop_rows = self.MysqlTargetDb.mysql_execute(sql_drop_view)
            view_rows = self.MysqlTargetDb.mysql_execute(sql_view)

    # 先创建所有视图同名的临时表，解决视图创建时依赖的视图不存在的问题
    def mysql_target_view_tmp(self, to_views_tmp_ddl):
        for to_view, to_view_ddl in to_views_tmp_ddl.items():
            fields_defination = ['`' + field[0] + '` tinyint not null' for field in to_view_ddl]
            sql_view_tmp = 'create table `' + self.to_db + '`.`' + to_view + '` (' + ','.join(fields_defination) + ') engine=myisam'
            view_tmp_rows = self.MysqlTargetDb.mysql_execute(sql_view_tmp)

    # 创建存储过程
    def mysql_target_procedure(self, to_procedures_ddl):
        for to_procedure, to_procedure_ddl in to_procedures_ddl.items():
            print('[DBM] Create procedure `' + to_procedure + '`')
            sql_procedure = 'create procedure `' + self.to_db + '`.' + to_procedure_ddl[2].split('PROCEDURE', 1)[1]
            procedure_rows = self.MysqlTargetDb.mysql_execute(sql_procedure)

    # 创建函数(set global log_bin_trust_function_creators=on 允许创建函数，需要super权限)
    def mysql_target_function(self, to_functions_ddl):
        sql_show_variable = "show variables like 'log_bin_trust_function_creators'"
        res_variable = self.MysqlTargetDb.mysql_select(sql_show_variable)
        if res_variable[0][1] == 'OFF':
            sql_variable = 'set global log_bin_trust_function_creators=on'
            variable_rows = self.MysqlTargetDb.mysql_execute(sql_variable)
            for to_function, to_function_ddl in to_functions_ddl.items():
                print('[DBM] Create function `' + to_function + '`')
                sql_function = 'create  function `' + self.to_db + '`.' + to_function_ddl[2].split('FUNCTION', 1)[1]
                function_rows = self.MysqlTargetDb.mysql_execute(sql_function)
            sql_variable = 'set global log_bin_trust_function_creators=off'
            variable_rows = self.MysqlTargetDb.mysql_execute(sql_variable)
        else:
            for to_function, to_function_ddl in to_functions_ddl.items():
                print('[DBM] Create function `' + to_function + '`')
                sql_function = 'create  function `' + self.to_db + '`.' + to_function_ddl[2].split('FUNCTION', 1)[1]
                function_rows = self.MysqlTargetDb.mysql_execute(sql_function)

    # 传输数据到目标表
    # 增加附带列名的插入，用于源库和目标库的表结构不完全一致的情况，added by wl_lw at 20200618
    def insert_target_data(self, to_table, data, columns=None):
        if data:
            str_list = ['%s' for i in range(len(data[0]))]
            value_str = ','.join(str_list)
            if not columns:
                insert_sql = 'insert into `' + self.to_db + '`.`' + to_table + '` values (' + value_str + ')'
            else:
                column_str = '`,`'.join(columns)
                insert_sql = 'insert into `' + self.to_db + '`.`' + to_table + '`(`' + column_str + '`) values (' + value_str + ')'
            # 默认情况下，自增列插入0或null时，该列使用auto_increment自增；修改sql_mode使得插入0时就插入数字0，以匹配阿里云的配置。
            self.MysqlTargetDb.mysql_execute_no_trans("set sql_mode='NO_AUTO_VALUE_ON_ZERO'")
            data_rows = self.MysqlTargetDb.mysql_executemany(insert_sql, data)
            return data_rows
        else:
            return 0

    # 目标库执行sql
    def mysql_target_execute(self, sql):
        affect_rows = self.MysqlTargetDb.mysql_execute(sql)
        return affect_rows

    # 目标库执行sql (no transaction)
    def mysql_target_execute_no_trans(self, sql):
        affect_rows = self.MysqlTargetDb.mysql_execute_no_trans(sql)
        return affect_rows

    # 禁用mysql外键
    def mysql_target_disable_fk(self):
        self.MysqlTargetDb.mysql_execute('set foreign_key_checks=0')

    # 启用mysql外键
    def mysql_target_enable_fk(self):
        self.MysqlTargetDb.mysql_execute('set foreign_key_checks=1')

    def close(self):
        self.MysqlTargetDb.mysql_execute_no_trans('set foreign_key_checks=1')
        self.MysqlTargetDb.close()


# @pysnooper.snoop()
# 目标库是mysql、源库是oracle时的表结构转换
class MysqlMetadataMapping(object):
    def __init__(self, source_type, from_table, res_pk):
        self.source_type = source_type
        self.table_name = from_table
        self.res_pk = res_pk
        if self.source_type.lower() != 'oracle':
            print('[DBM] ERROR：目前不支持' + self.source_type + '->mysql的元数据转换!')
            sys.exit(1)

    # 数据类型对应关系
    def _column_mapping(self, data_type, data_length, data_precision, data_scale):
        if data_type == 'NUMBER' and data_scale == 0 and data_precision and data_precision <= 3:
            mysql_type = 'tinyint'
        elif data_type == 'NUMBER' and data_scale == 0 and data_precision and data_precision <= 5:
            mysql_type = 'smallint'
        elif data_type == 'NUMBER' and data_scale == 0 and data_precision and data_precision <= 7:
            mysql_type = 'mediumint'
        elif data_type == 'NUMBER' and data_scale == 0 and data_precision and data_precision <= 10:
            mysql_type = 'int'
        elif data_type == 'NUMBER' and data_scale == 0 and data_precision and data_precision > 10:
            mysql_type = 'bigint'
        elif data_type == 'NUMBER' and data_scale == 0 and not data_precision:
            mysql_type = 'int'
        elif data_type == 'NUMBER' and data_scale != 0 and data_precision and data_scale:
            mysql_type = 'decimal(' + str(data_precision) + ',' + str(data_scale) + ')'
        elif data_type == 'NUMBER' and not data_precision and not data_scale:
            mysql_type = 'bigint'
        elif data_type == 'FLOAT':
            mysql_type = 'double'
        if data_type == 'DATE':
            mysql_type = 'datetime'
        elif data_type.startswith('TIMESTAMP') and data_scale > 0:
            mysql_type = 'datetime(' + str(data_scale) + ')'
        elif data_type.startswith('TIMESTAMP') and (not data_scale or data_scale == 0):
            mysql_type = 'datetime'
        if data_type == 'CHAR':
            mysql_type = 'char(' + str(data_length) + ')'
        elif data_type == 'VARCHAR2':
            mysql_type = 'varchar(' + str(data_length) + ')'
        elif data_type == 'CLOB' and data_length < 4000:
            mysql_type = 'varchar(' + str(data_length) + ')'
        elif data_type == 'CLOB' and data_length >= 4000:
            mysql_type = 'longtext'
        if data_type == 'BLOB' or data_type == 'RAW':
            mysql_type = 'blob'
        return mysql_type

    # 获取表的主键值
    def _pk_mapping(self):
        # 获取主键列
        pk_list = list(filter(lambda x: x.get('table_name') == self.table_name, self.res_pk))
        pk_columns_info = pk_list[0].get('column_name') if pk_list else None
        return pk_columns_info

    # 字段转换
    def column_convert(self, res_columns, res_comments):
        # 列转换
        mysql_columns = []
        for record in res_columns:
            for comment_row in res_comments:
                if comment_row.get('column_name') and record.get('column_name') == comment_row.get('column_name'):
                    record.update(comment=comment_row.get('comments'))
            data_type = record.get('data_type')
            data_length = record.get('data_length')
            data_precision = record.get('data_precision')
            data_scale = record.get('data_scale')
            mysql_type = self._column_mapping(data_type, data_length, data_precision, data_scale)
            mysql_nullable = 'YES' if record.get('nullable') == 'Y' else 'NO'
            mysql_record = dict()
            mysql_record.setdefault('field', record.get('column_name'))
            mysql_record.setdefault('type', mysql_type)
            mysql_record.setdefault('collation', None)
            mysql_record.setdefault('null', mysql_nullable)
            pk_columns_info = self._pk_mapping()
            pk_column_list = pk_columns_info.split(',') if pk_columns_info else []
            mysql_record.setdefault('key', 'PRI') if record.get('column_name') in pk_column_list else mysql_record.setdefault('key', None)
            if record.get('data_default') and mysql_type.startswith('datetime') and record.get('data_default') != 'NULL':
                default_value = 'current_timestamp' + mysql_type.lstrip('datetime')
                mysql_record.setdefault('default', default_value)
            elif record.get('data_default') and mysql_type.startswith('timestamp') and record.get('data_default') != 'NULL':
                default_value = 'current_timestamp' + mysql_type.lstrip('timestamp')
                mysql_record.setdefault('default', default_value)
            elif record.get('data_default') and not mysql_type.startswith('datetime') and not mysql_type.startswith('timestamp') and record.get('data_default') != 'NULL':
                mysql_record.setdefault('default', record.get('data_default'))
            elif record.get('data_default') == 'NULL':
                mysql_record.setdefault('default', None)
            elif not record.get('data_default'):
                mysql_record.setdefault('default', None)
            mysql_record.setdefault('extra', None)
            mysql_record.setdefault('privileges', None)
            mysql_record.setdefault('comment', record.get('comment'))
            mysql_columns.append(mysql_record)
        return mysql_columns

    # 表的comment信息转换
    def table_comment_convert(self, res_comments):
        mysql_tablestatus = list(filter(lambda x: not x.get('column_name'), res_comments))
        return mysql_tablestatus

    # 表上的索引转换
    # @pysnooper.snoop()
    def index_convert(self, index_column_info, res_columns):
        # 去除主键索引
        pk_columns_info = self._pk_mapping()
        mysql_indexes = list(filter(lambda x: x.get('column_name') != pk_columns_info and x.get('column_name'), index_column_info))
        # 针对从Oracle转换过来的BLOB/TEXT字段，取前100字符创建索引,避免mysql error 1170报错
        for index in mysql_indexes:
            index_column_list = index.get('column_name').split(',')
            index_column_list_tmp = list(map(lambda x: x.strip(), index_column_list))
            for record in res_columns:
                orignal_column = record.get('column_name') if (record.get('data_type') == 'BLOB' or record.get('data_type') == 'CLOB' or record.get('data_type') == 'RAW') else None
                if orignal_column and orignal_column in index_column_list_tmp:
                    i = index_column_list_tmp.index(orignal_column)
                    index_column_list_tmp[i] = orignal_column + '(100)'
                index_column_new = ','.join(index_column_list_tmp)
            index.update(column_name=index_column_new)
        return mysql_indexes

    # 表上的外键转换
    @staticmethod
    def fk_convert(res_fk):
        mysql_fk = []
        for fk_record in res_fk:
            fk_record.update(referenced_table_name=fk_record.get('r_table_name'), referenced_column_name=fk_record.get('r_column_name'), update_rule=fk_record.get('delete_rule'))
            fk_record.pop('r_table_name')
            fk_record.pop('r_column_name')
            fk_record.pop('r_constraint_name')
            mysql_fk.append(fk_record)
        return mysql_fk


# 源库是mysql时的查询和目标库插入方法(for 并行同步)
def mysql_select_insert(sql_info, source_db_info, target_db_info):
    mysql_source = MysqlSource(**source_db_info)
    # 判断目标数据库类型，根据不同类型调用不同的插入方法(不同类型数据库的插入方法名保持一致)
    target_db_type = target_db_info.get('target_db_type')
    if target_db_type == 'mysql':
        multi_db_target = MysqlTarget(**target_db_info)
    elif target_db_type == 'oracle':
        multi_db_target = OracleTarget(**target_db_info)
    else:
        pass
    # 优化同步逻辑，每进程内循环多次同步数据，每次取batch_rows行记录 --modified at 20190902
    from_db = sql_info.get('from_db')
    from_table = sql_info.get('from_table')
    to_table = sql_info.get('to_table')
    parallel_key = sql_info.get('parallel_key')
    from_rowid = sql_info.get('from_rowid')
    to_rowid = sql_info.get('to_rowid')
    max_key = sql_info.get('max_key')
    # 每进程每次同步10w行记录，分批循环处理
    batch_rows = 100000
    batch_num = math.ceil((to_rowid - from_rowid)/batch_rows)
    # 每进程内最大循环批次为100，超过100时则增加每次同步记录数 --modified at 20191009
    if batch_num > 100:
        batch_num = 100
        batch_rows = math.ceil((to_rowid - from_rowid)/100)
    insert_rows_list = []
    for b in range(batch_num):
        batch_from = from_rowid + (b * batch_rows)
        batch_to = to_rowid if (b == batch_num -1) else (from_rowid + ((b + 1) * batch_rows))
        if batch_to >= max_key:
            sql_select = 'select /* !40001 SQL_NO_CACHE */ * from `' + from_db + '`.`' + from_table + '` where `' + \
                         parallel_key + '` >= ' + str(batch_from) + ' and `' + parallel_key + '` <= ' + str(max_key)
        else:
            sql_select = 'select /* !40001 SQL_NO_CACHE */ * from `' + from_db + '`.`' + from_table + '` where `' + parallel_key + '` >= ' + str(batch_from) + ' and `' + parallel_key + '` < ' + str(batch_to)
        #print(sql_select)
        sql_data = mysql_source.mysql_source_data(sql_select)
        sql_columns = mysql_source.mysql_source_column(sql_select)
        insert_rows = multi_db_target.insert_target_data(to_table, sql_data, columns=sql_columns)
        insert_rows_list.append(insert_rows)
    total_rows = reduce(lambda x, y: x + y, insert_rows_list)
    mysql_source.close()
    multi_db_target.close()
    return total_rows


# 源库是mysql时的并行/串行数据处理
class MysqlDataMigrate(object):
    def __init__(self, source_db_info, target_db_info):
        self.source_db_info = source_db_info
        self.target_db_info = target_db_info
        self.mysql_source = MysqlSource(**source_db_info)
        # 判断目标数据库类型，根据不同类型调用不同的插入方法(不同类型数据库的插入方法名保持一致)
        self.target_db_type = target_db_info.get('target_db_type')
        if self.target_db_type == 'mysql':
            self.multi_db_target = MysqlTarget(**target_db_info)
        elif self.target_db_type == 'oracle':
            self.multi_db_target = OracleTarget(**target_db_info)
        else:
            pass

    # 源库是mysql时的数据同步串行处理方法
    def mysql_serial_migrate(self, from_table, to_table):
        from_db = self.source_db_info.get('db')
        to_db = self.target_db_info.get('db')
        sql_select = 'select /* !57800 SQL_NO_CACHE max_execution_time=3600000 */ * from `' + from_db + '`.`' + from_table + '`'
        sql_info = {'table_name': to_table, 'sql_statement': sql_select}
        print('[DBM] Inserting data into table `' + to_table + '`')
        # 使用MysqlOperateIncr子类分批获取数据，降低客户端内存压力和网络传输压力
        mysql_source_incr = MysqlOperateIncr(**self.source_db_info)
        # 串行获取数据时每批次获取数据量
        arraysize = 100000
        # 设置session级SQL最大执行时间为1小时
        # sql_max_exec_time = 'set max_execution_time=3600000'
        # mysql_source_incr.mysql_execute(sql_max_exec_time)
        res_data_incr = mysql_source_incr.mysql_select_incr(sql_select, arraysize=arraysize)
        sql_columns = mysql_source_incr.mysql_select_column(sql_select)
        insert_rows_list = []
        while True:
            data_incr = next(res_data_incr)
            if data_incr:
                insert_rows = self.multi_db_target.insert_target_data(to_table, data_incr, columns=sql_columns)
                insert_rows_list.append(insert_rows)
            else:
                break
        total_rows = reduce(lambda x, y: x + y, insert_rows_list) if insert_rows_list else 0
        return total_rows

    # 源库是mysql时的数据同步的并行处理方法
    def mysql_parallel_migrate(self, from_table, to_table, final_parallel, parallel_key=None, parallel_method='multiprocess'):
        # 分批数据
        from_db = self.source_db_info.get('db')
        to_db = self.target_db_info.get('db')
        # Mysql_Operate = MysqlSourceTarget(self.source_db_info, self.target_db_info)
        sql_max_min = 'select max(`' + parallel_key + '`), min(`' + parallel_key + '`) from `' + from_db + '`.`' + from_table + '`'
        res_max_min = self.mysql_source.mysql_source_data(sql_max_min)
        max_key = res_max_min[0][0]
        min_key = res_max_min[0][1]
        # per_rows为每进程处理的数据量
        per_rows = math.ceil((max_key - min_key + 1)/final_parallel)

        #源库select语句
        sql_select_list = []
        for p in range(final_parallel):
            from_rowid = min_key + (p * per_rows)
            to_rowid = min_key + ((p + 1) * per_rows)
            sql_info = {'from_db': from_db, 'from_table': from_table, 'to_table': to_table, 'parallel_key': parallel_key, 'per_rows': per_rows, 'from_rowid': from_rowid, 'to_rowid': to_rowid, 'max_key': max_key}
            sql_select_list.append(sql_info)
            #sql_select = 'select /* !40001 SQL_NO_CACHE */ * from `' + from_db + '`.`' + from_table + '` where `' + parallel_key + '` >= ' + \
            #             str(min_key + (p * per_rows)) + ' and `' + parallel_key + '` < ' + str(min_key + ((p + 1) * per_rows))
            #select_insert_dict = {'table_name': to_table, 'sql_statement': sql_select}
            #sql_select_list.append(select_insert_dict)
        #print(sql_select_list)

        print('[DBM] Inserting data into table `' + to_table + '`')
        #多进程
        if parallel_method == 'multiprocess':
            with Pool(final_parallel) as p:
                insert_rows = p.map(partial(mysql_select_insert, source_db_info=self.source_db_info, target_db_info=self.target_db_info), sql_select_list)
            total_rows = reduce(lambda x, y: x + y, insert_rows)
            #print(insert_rows)
            #print(total_rows)
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

    # 源库是mysql时的数据同步串行/并行选择
    def mysql_parallel_flag(self, from_table, res_tablestatus, res_columns, parallel=0):
        #用户指定并发数或行数大于10w或空间使用大于100M，并且主键列为int时，设置并发
        parallel_flag = 0 if parallel == 0 else 1
        pri_keys = [(record.get('field'), record.get('type')) for record in res_columns if
                    (record['key'] == 'PRI' or record['key'] == 'pri')]
        pri_key_types = [pri_key[1] for pri_key in pri_keys]
        if not pri_key_types:
            parallel_key = None
            parallel_flag = 0
        elif (res_tablestatus[0].get('rows') > 100000 or res_tablestatus[0].get('data_length') > 100000000) and 'int' in [key_type for key_type in pri_key_types][0]:
            # 并行主键
            parallel_key = [k[0] for k in pri_keys if 'int' in k[1]][0]
            parallel_flag = 1 if parallel_key else 0
        else:
            parallel_key = None
            parallel_flag = 0
        # 设置并行方式(linux为多进程，windows为多线程) 和 最大并发数, （多线程速度较慢，这里全改为多进程）
        #parallel_method = 'multiprocess' if os.name == 'posix' else 'threading'
        parallel_method = 'multiprocess'
        max_parallel = cpu_count()

        #计算实际并发数=用户指定并发数(用户指定并发数最大为cpu*2；如未指定，实际并发数=round(表记录数/10w)
        if parallel == 0:
            auto_parallel = round(int(res_tablestatus[0].get('rows'))/100000)
            final_parallel = max(min(auto_parallel, max_parallel), 1)
        else:
            final_parallel = min(parallel, max_parallel*2)
        return parallel_flag, final_parallel, parallel_key, parallel_method

    # 源库是mysql时的增量数据同步串行处理方法
    def mysql_incr_serial_migrate(self, from_table, to_table, incremental_method=None, where_clause=None):
        from_db = self.source_db_info.get('db')
        to_db = self.target_db_info.get('db')
        sql_select_original = 'select /* !57800 SQL_NO_CACHE max_execution_time=3600000 */ * from `' + from_db + '`.`' + from_table + '`'
        sql_select = sql_select_original + ' ' + where_clause if incremental_method == 'where' else sql_select_original
        sql_info = {'table_name': to_table, 'sql_statement': sql_select}
        print('[DBM] Inserting data into table `' + to_table + '`')
        # 使用MysqlOperateIncr子类分批获取数据，降低客户端内存压力和网络传输压力
        mysql_source_incr = MysqlOperateIncr(**self.source_db_info)
        # 串行获取数据时每批次获取数据量
        arraysize = 10000
        # 设置session级SQL最大执行时间为1小时
        # sql_max_exec_time = 'set max_execution_time=3600000'
        # mysql_source_incr.mysql_execute(sql_max_exec_time)
        columns = mysql_source_incr.mysql_select_column(sql_select)
        res_data_incr = mysql_source_incr.mysql_select_incr(sql_select, arraysize=arraysize)
        insert_rows_list = []
        while True:
            data_incr = next(res_data_incr)
            if data_incr:
                insert_rows = self.multi_db_target.insert_target_data(to_table, data_incr, columns=columns)
                insert_rows_list.append(insert_rows)
            else:
                break
        total_rows = reduce(lambda x, y: x + y, insert_rows_list) if insert_rows_list else 0
        return total_rows

    def close(self):
        self.mysql_source.close()
        self.multi_db_target.close()
