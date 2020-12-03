#! /usr/bin/env python3
# -*- coding:utf-8 -*-

'''
mssql迁移源库和目标库类
'''

import traceback, sys, datetime, math
from functools import reduce, partial
from multiprocessing import cpu_count, Process, Pool
from mssql_operate import MssqlOperate
from comm_decorator import performance, MyThread


# 源库为mssql时获取数据
class MssqlSource(object):
    # 检查目标库连接
    def __init__(self, **db_info):
        self.mssql_source = MssqlOperate(db_info)
        if self.mssql_source.check_db_conn() == 0:
            print('DBM Error: can not connect to target db: ' + db_info.get('host') + ':' +
                  str(db_info.get('port')) + '/' + db_info.get('db'))
            traceback.print_exc()
            sys.exit(1)

    # 检查配置文件中的表在源库是否存在，未配置则全部表同步
    def source_table_check(self, *source_tables):
        res_tables = self.mssql_source.execute("""select lower(name) from sysobjects where xtype='U' order by name""")
        all_table_list = [table[0] for table in res_tables]
        source_tables = list(map(lambda x: x.lower(), source_tables))
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

    # 获取源表元数据(目前仅用于判断是否并行获取数据,不涉及表结构迁移--20201203)
    def mssql_source_table(self, from_table):
        from_table = from_table.lower()
        res_tablestatus = self.mssql_source.execute_dict("select lower(t.name) as table_name, i.rows as num_rows "
                                                         " from sys.tables as t, sysindexes as i "
                                                         " where t.object_id = i.id and i.indid <= 1 "
                                                         " and lower(t.name) = lower('{TABLE_NAME}')".format(TABLE_NAME=from_table))
        res_columns = self.mssql_source.execute_dict("select lower(c.table_name) as table_name, "
                                                     " c.column_name as column_name, c.data_type as data_type, "
                                                     " c.character_maximum_length as data_length, "
                                                     " c.numeric_precision as numberic_precision, "
                                                     " c.numeric_scale as numeric_scale, "
                                                     " c.is_nullable as nullable, c.column_default as column_default "
                                                     " from information_schema.columns c "
                                                     " where lower(table_name) = lower('{TABLE_NAME}')".format(TABLE_NAME=from_table))
        return res_tablestatus, res_columns

    # 获取源表索引(包括主键对应的索引)
    def mssql_source_index(self, from_table):
        '''
        :param from_table:
        :return: index_column_info <class 'list'>: [{'table': 'xxx', 'key_name': 'xxx', 'non_unique': 0, 'column_name': 'col1,col2'}]
        '''
        from_table = from_table.lower()
        sql_indexes = """
                      SELECT lower(t.name) as 'table', lower(i.name) as 'key_name',
                             CASE
                                 WHEN i.is_unique = 1 THEN
                                     0
                                 ELSE
                                     1
                             END AS 'non_unique',
                             SUBSTRING(column_names, 1, LEN(column_names) - 1) as 'column_name',
                             CASE
                                 WHEN i.[type] = 1 THEN
                                     '聚集索引'
                                 WHEN i.[type] = 2 THEN
                                     '非聚集索引'
                                 WHEN i.[type] = 3 THEN
                                     'XML索引'
                                 WHEN i.[type] = 4 THEN
                                     '空间索引'
                                 WHEN i.[type] = 5 THEN
                                     '聚簇列存储索引'
                                 WHEN i.[type] = 6 THEN
                                     '非聚集列存储索引'
                                 WHEN i.[type] = 7 THEN
                                     '非聚集哈希索引'
                             END AS 'index_type'
                        FROM sys.objects t
                        INNER JOIN sys.indexes i
                          ON t.object_id = i.object_id
                        CROSS APPLY (SELECT col.[name] + ', '
                                       FROM sys.index_columns ic
                                       INNER JOIN sys.columns col
                                         ON ic.object_id = col.object_id
                                        AND ic.column_id = col.column_id
                                      WHERE ic.object_id = t.object_id
                                        AND ic.index_id = i.index_id
                                      ORDER BY col.column_id
                                      FOR XML PATH('')) D(column_names)
                       WHERE t.is_ms_shipped <> 1
                         AND index_id > 0
                         AND lower(t.name) = lower('{TABLE_NAME}')
                       ORDER BY i.is_unique desc
        """
        res_indexes = self.mssql_source.execute_dict(sql_indexes.format(TABLE_NAME=from_table))

        index_column_info = res_indexes if isinstance(res_indexes, list) else list(res_indexes)
        return index_column_info

    # 增量多次获取表数据
    def mssql_source_data_incr(self, sql, arraysize=10000):
        res_data = self.mssql_source.execute_incr(sql, arraysize=arraysize)
        return res_data

    # 全量一次性获取数据
    def mssql_source_data_all(self, sql):
        desc, res_data = self.mssql_source.execute_description(sql)
        return desc, res_data

    # 获取源表列名，用于源库和目标库的表结构不完全一致的情况 added by wl_lw at 20200618
    def mssql_source_columns(self, sql):
        columns = self.mssql_source.mssql_columns(sql)
        return columns

    # 返回mssql数据库连接cursor
    def cursor(self):
        return self.mssql_source.cursor()

    # 单独的获取mssql数据的静态方法
    @staticmethod
    def mssql_select(sql, db_info):
        mssql_source = MssqlOperate(db_info)
        res = mssql_source.execute(sql)
        mssql_source.close()
        return res

    # 关闭连接
    def close(self):
        self.mssql_source.close()


# 目标库为mssql时装载数据
class MssqlTarget(object):
    # 检查目标库连接
    def __init__(self, **db_info):
        self.mssql_target = MssqlOperate(db_info)
        if self.mssql_target.check_db_conn() == 0:
            print('DBM Error: can not connect to target db: ' + db_info.get('host') + ':' +
                  str(db_info.get('port')) + '/' + db_info.get('db'))
            traceback.print_exc()
            sys.exit(1)

    # 增加附带列名的插入，用于源库和目标库的表结构不完全一致的情况，added by wl_lw at 20201202
    def insert_target_data(self, to_table, data, columns=None):
        if data:
            str_list = [(':' + str(i)) for i in range(len(data[0]))]
            value_str = ','.join(str_list)
            if not columns:
                insert_sql = 'insert into ' + to_table + ' values (' + value_str + ')'
            else:
                column_str = ','.join(columns)
                insert_sql = 'insert into ' + to_table + '(' + column_str + ') values (' + value_str + ')'
            data = list(data) if not isinstance(data, list) else data
            data_rows = self.mssql_target.insertbatch(insert_sql, data)
            return data_rows
        else:
            return 0

    # 关闭连接
    def close(self):
        self.mssql_target.close()


# 源库是mssql时的查询和目标库插入方法(for 并行同步)
def mssql_select_insert(sql_info, source_db_info, target_db_info):
    mssql_source = MssqlSource(**source_db_info)
    # 判断目标数据库类型，根据不同类型调用不同的插入方法(不同类型数据库的插入方法名保持一致)
    target_db_type = target_db_info.get('target_db_type')
    if target_db_type == 'mysql':
        # 使用时引用，避免交叉引用
        from mysql_migrate import MysqlTarget
        multi_db_target = MysqlTarget(**target_db_info)
    elif target_db_type == 'oracle':
        from oracle_migrate import OracleTarget
        multi_db_target = OracleTarget(**target_db_info)
    elif target_db_type == 'mssql':
        multi_db_target = MssqlTarget(**target_db_info)
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
            sql_select = 'select * from ' + from_table + ' where ' + parallel_key + ' >= ' + str(
                batch_from) + ' and ' + parallel_key + ' <= ' + str(max_key)
        else:
            sql_select = 'select * from ' + from_table + ' where ' + parallel_key + ' >= ' + str(
                batch_from) + ' and ' + parallel_key + ' < ' + str(batch_to)
        sql_data = mssql_source.mssql_source_data_all(sql_select)
        sql_columns = mssql_source.mssql_source_columns(sql_select)
        insert_rows = multi_db_target.insert_target_data(to_table, sql_data, columns=sql_columns)
        insert_rows_list.append(insert_rows)
    total_rows = reduce(lambda x, y: x + y, insert_rows_list)
    mssql_source.close()
    multi_db_target.close()
    return total_rows


# 源库是mssql时的并行/串行数据处理
class MssqlDataMigrate(object):
    def __init__(self, source_db_info, target_db_info):
        self.source_db_info = source_db_info
        self.target_db_info = target_db_info
        self.mssql_source = MssqlSource(**source_db_info)
        # 判断目标数据库类型，根据不同类型调用不同的插入方法(不同类型数据库的插入方法名保持一致)
        self.target_db_type = target_db_info.get('target_db_type')
        if self.target_db_type == 'mysql':
            from mysql_migrate import MysqlTarget
            self.multi_db_target = MysqlTarget(**target_db_info)
        elif self.target_db_type == 'oracle':
            from oracle_migrate import OracleTarget
            self.multi_db_target = OracleTarget(**target_db_info)
        elif self.target_db_type == 'mssql':
            self.multi_db_target = MssqlTarget(**target_db_info)
        else:
            pass

    # 源库是mssql时的数据同步串行处理方法
    def mssql_serial_migrate(self, from_table, to_table):
        print('[DBM] Inserting data into table `' + to_table + '`')
        sql_select = 'select * from ' + from_table + ' t'
        # 串行获取数据,每批10w行
        res_data_incr = self.mssql_source.mssql_source_data_incr(sql_select)
        sql_columns = self.mssql_source.mssql_source_columns(sql_select)
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

    # 源库是mssql时的数据同步的并行处理方法
    def mssql_parallel_migrate(self, from_table, to_table, final_parallel, parallel_key=None, parallel_method='multiprocess'):
        # 分批数据
        from_db = self.source_db_info.get('db')
        to_db = self.target_db_info.get('db')
        sql_max_min = 'select max(' + parallel_key + '), min(' + parallel_key + ') from ' + from_table
        res_max_min = self.mssql_source.mssql_source_data_all(sql_max_min)
        max_key = res_max_min[0][0]
        min_key = res_max_min[0][1]
        # per_rows为每进程处理的数据量
        per_rows = math.ceil((max_key - min_key + 1)/final_parallel)

        # 源库select语句
        sql_select_list = []
        for p in range(final_parallel):
            from_rowid = min_key + (p * per_rows)
            to_rowid = min_key + ((p + 1) * per_rows)
            sql_info = {'from_db': from_db, 'from_table': from_table, 'to_table': to_table, 'parallel_key': parallel_key, 'per_rows': per_rows, 'from_rowid': from_rowid, 'to_rowid': to_rowid, 'max_key': max_key}
            sql_select_list.append(sql_info)
        print('[DBM] Inserting data into table `' + to_table + '`')
        # 多进程
        if parallel_method == 'multiprocess':
            with Pool(final_parallel) as p:
                insert_rows = p.map(partial(mssql_select_insert, source_db_info=self.source_db_info, target_db_info=self.target_db_info), sql_select_list)
            total_rows = reduce(lambda x, y: x + y, insert_rows)
            # print(insert_rows)
            # print(total_rows)
        else:
            # 多线程
            thread_list = []
            result_list = []
            for t in range(final_parallel):
                sql_info = sql_select_list[t]
                sthread = MyThread(mssql_select_insert, (sql_info, self.source_db_info, self.target_db_info))
                thread_list.append(sthread)
            for t in thread_list:
                t.start()
            for t in thread_list:
                t.join()
            for t in thread_list:
                result_list.append(sthread.get_result())
            total_rows = reduce(lambda x, y: x + y, result_list)
        return total_rows

    # 源库是mssql时的数据同步串行/并行选择
    def mssql_parallel_flag(self, from_table, index_column_info, res_tablestatus, res_columns, parallel=0):
        # 用户指定并发数或行数大于10w，并且主键列为int时，设置并发
        parallel_flag = 0 if parallel == 0 else 1
        pri_keys = []
        for i in index_column_info:
            pri_col = i.get('column_name')
            col_list = list(filter(lambda x: x.get('column_name') == pri_col and x.get('nullable') == 'NO' and
                                             x.get('data_type') == 'int', res_columns))
            pri_keys.append(col_list[0].get('column_name')) if col_list else None
        if res_tablestatus[0].get('num_rows') > 100000 and pri_keys:
            # 并行主键
            parallel_key = pri_keys[0]
            parallel_flag = 1
        else:
            parallel_key = None
            parallel_flag = 0
        # 设置并行方式(linux为多进程，windows为多线程) 和 最大并发数, （多线程速度较慢，这里全改为多进程）
        # parallel_method = 'multiprocess' if os.name == 'posix' else 'threading'
        parallel_method = 'multiprocess'
        max_parallel = cpu_count()
        # 计算实际并发数=用户指定并发数(用户指定并发数最大为cpu*2；如未指定，实际并发数=round(表记录数/10w)
        if parallel == 0:
            auto_parallel = round(int(res_tablestatus[0].get('num_rows'))/100000)
            final_parallel = max(min(auto_parallel, max_parallel), 1)
        else:
            final_parallel = min(parallel, max_parallel*2)
        return parallel_flag, final_parallel, parallel_key, parallel_method

    # 源库是mssql时的增量数据同步串行处理方法
    def mssql_incr_serial_migrate(self, from_table, to_table, incremental_method=None, where_clause=None):
        from_db = self.source_db_info.get('db')
        to_db = self.target_db_info.get('db')
        sql_select_original = 'select * from ' + from_table + ' t'
        sql_select = sql_select_original + ' ' + where_clause if incremental_method == 'where' else sql_select_original
        # 串行获取数据,每批10w行
        res_data_incr = self.mssql_source.mssql_source_data_incr(sql_select)
        sql_columns = self.mssql_source.mssql_source_columns(sql_select)
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

    def close(self):
        self.mssql_source.close()
        self.multi_db_target.close()




