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
    # 检查源数据库连接
    def __init__(self, **source_db_info):
        self.hostname = source_db_info.get('host')
        self.port = source_db_info.get('port')
        self.service_name = source_db_info.get('db')
        self.username = source_db_info.get('user')
        self.password = source_db_info.get('password')
        self.charset = source_db_info.get('charset')
        try:
            self.OracleSourceConn = DbOperate(self.hostname, self.port, self.username, self.password,
                                              service_name=self.service_name)
        except Exception as e:
            print('DBM Error: can not connect to target db: ' + source_db_info.get('host') + ':' +
                  str(source_db_info.get('port')) + '/' + source_db_info.get('db'))
            traceback.print_exc()
            sys.exit(1)

    # 检查配置文件中的表在源库是否存在，未配置则全部表同步
    def source_table_check(self, *source_tables):
        res_tables = self.OracleSourceConn.execute('select lower(table_name) as table_name from user_tables order by table_name')
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

    # 获取源表元数据
    def oracle_source_table(self, from_table):
        from_table = from_table.upper()
        res_tablestatus = self.OracleSourceConn.execute_dict("select lower(table_name) as table_name, logging, "
                                                             "partitioned, compression, compress_for, num_rows "
                                                             "from user_tables "
                                                             "where table_name = upper('{TABLE_NAME}')".format(TABLE_NAME=from_table))
        res_columns = self.OracleSourceConn.execute_dict("select lower(column_name) as column_name, column_id, "
                                                         "data_type, data_length, data_precision, data_scale, "
                                                         "nullable, data_default from user_tab_columns "
                                                         "where table_name = upper('{TABLE_NAME}')"
                                                         "order by column_id".format(TABLE_NAME=from_table))
        res_partitions = self.OracleSourceConn.execute_dict("select lower(partition_name) as partition_name, "
                                                            "partition_position, subpartition_count, high_value, "
                                                            "tablespace_name, logging, compression, compress_for "
                                                            "from user_tab_partitions "
                                                            "where table_name = upper('{TABLE_NAME}')"
                                                            "order by partition_position".format(TABLE_NAME=from_table))
        res_triggers = self.OracleSourceConn.execute_dict("select lower(trigger_name) as trigger_name, trigger_type, "
                                                          "triggering_event, base_object_type, "
                                                          "lower(table_name) as table_name, "
                                                          "lower(column_name) as column_name, referencing_names, "
                                                          "status, description, trigger_body "
                                                          "from user_triggers "
                                                          "where base_object_type='TABLE' "
                                                          "and table_name = upper('{TABLE_NAME}')".format(TABLE_NAME=from_table))
        res_segments = self.OracleSourceConn.execute_dict("select lower(t.table_name) as table_name, "
                                                          "max(t.num_rows) as num_rows, "
                                                          "decode(sum(s.bytes),null,0,sum(s.bytes)) as data_length "
                                                          "from user_segments s, user_tables t "
                                                          "where s.segment_name(+) = t.table_name "
                                                          "and t.table_name = upper('{TABLE_NAME}') "
                                                          "group by t.table_name".format(TABLE_NAME=from_table))
        return res_tablestatus, res_partitions, res_columns, res_triggers, res_segments

    # 获取源表索引(包括主键对应的索引)
    def oracle_source_index(self, from_table):
        from_table = from_table.upper()
        sql_indexes = """
        select lower(i.index_name) as index_name,
               lower(i.table_name) as table_name,
               i.uniqueness as uniqueness,
               i.prefix_length as prefix_length,
               i.logging as logging,
               i.visibility as visibility,
               lower(c.column_name) as column_name,
               c.descend as descend,
               c.column_position as column_position,
               e.column_expression as column_expression
          from user_indexes i, user_ind_columns c, user_ind_expressions e
         where i.index_name = c.index_name
           and c.index_name = e.index_name(+)
           and c.column_position = e.column_position(+)
           and i.table_name = '{TABLE_NAME}'
           and i.index_type <> 'LOB'
        """
        res_indexes = self.OracleSourceConn.execute_dict(sql_indexes.format(TABLE_NAME=from_table))

        # 对返回的索引列表排序
        res_indexes.sort(key=itemgetter('index_name', 'column_position'))
        # 获取索引（唯一）和[多]列的对应关系，组成新的dict
        # dict.setdefault(key,default)：当key存在就返回key对应的value，key不存在时就设置为default,有点类似get()
        index_column_map = {}
        for row_index in res_indexes:
            index_column_map.setdefault(row_index.get('index_name'), []).append(
                row_index.get('column_expression') if row_index.get('column_expression') else row_index.get(
                    'column_name') + (' desc' if row_index.get('descend') != 'ASC' else ''))
        # 对比新dict[index_column_def] 和 原[res_indexes]，获取索引详细信息,组成index_column_info
        c = []
        for key, value in index_column_map.items():
            temp = {}
            for row_index in res_indexes:
                if key == row_index.get('index_name'):
                    temp.setdefault('table', row_index.get('table_name'))
                    temp.setdefault('key_name', key)
                    temp.setdefault('non_unique', 1 if row_index.get('uniqueness') == 'NONUNIQUE' else 0)
                    temp.setdefault('column_name', ", ".join(value))
                    c.append(temp)
        # print(c)
        # 去重
        index_column_info = []
        [index_column_info.append(i) for i in c if i not in index_column_info]
        return index_column_info

    # 获取源表数据
    def oracle_source_data(self, sql):
        res_data = self.OracleSourceConn.execute(sql)
        return res_data

    # 增量获取表数据
    def oracle_source_data_incr(self, sql, arraysize=100000):
        res_data = self.OracleSourceConn.oracle_select_incr(sql, arraysize=arraysize)
        return res_data

    # 获取源表涉及的主键和外键
    def oracle_source_pk_fk(self, from_tables):
        sql_constraints="""
        select lower(c.constraint_name) as constraint_name,
               c.constraint_type as constraint_type,
               lower(c.table_name) as table_name,
               lower(col.column_name) as column_name,
               lower(c.r_constraint_name) as r_constraint_name,
               lower(ref_col.table_name) as r_table_name,
               lower(ref_col.column_name) as r_column_name,
               c.delete_rule as delete_rule,
               c.status as status,
               c.deferrable as deferrable,
               c.deferred as deferred,
               c.validated as validated,
               lower(c.index_name) as index_name
          from user_constraints c,
               (select constraint_name,
                       table_name,
                       listagg(column_name, ',') within group(order by position) as column_name
                  from user_cons_columns
                 where constraint_name not like 'BIN%'
              group by constraint_name, table_name) col,
               (select constraint_name,
                       table_name,
                       listagg(column_name, ',') within group(order by position) as column_name
                  from user_cons_columns
                 where constraint_name not like 'BIN%'
              group by constraint_name, table_name) ref_col
         where c.constraint_name = col.constraint_name
           and c.r_constraint_name = ref_col.constraint_name(+)
           and c.constraint_type in ('R','F','P')
           and c.constraint_name not like 'BIN$%'
        order by table_name
        """
        res_constraints = self.OracleSourceConn.execute_dict(sql_constraints)
        # 获取需要同步的表的主键
        res_pk = list(filter(lambda x: x.get('constraint_type') == 'P' and x.get('table_name') in from_tables, res_constraints))
        # dict取子集
        pk_keys = {'constraint_name', 'table_name', 'column_name'}
        final_pk = {key: value for key, value in res_pk.items if key in pk_keys}

        # 获取需要同步的表的外键
        res_fk = list(filter(lambda x: x.get('constraint_type') == 'R' and x.get('table_name') in from_tables and x.get('r_table_name') in from_tables, res_constraints))
        # dict取子集
        fk_keys = {'constraint_name', 'table_name', 'column_name', 'r_constraint_name', 'r_table_name', 'r_column_name', 'delete_rule'}
        final_fk = {key: value for key, value in res_fk.items if key in fk_keys}
        return final_pk, final_fk

    # 获取源库的所有视图,存储过程，函数和routines
    def oracle_source_pkg(self):
        pass
        # to_do


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
                                              service_name=self.service_name)
            #self.OracleTargetConn = DbOperate(self.hostname, self.port, self.username, self.password,
            #                                  service_name=self.service_name, session_pool='Y', min=1, max=100,
            #                                  increment=4, charset=self.charset)
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
        res_tables = self.OracleTargetConn.execute('select lower(table_name) as table_name from user_tables')
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

    def oracle_target_close(self):
        self.OracleTargetConn.close()


# 源库是mysql时的查询和目标库插入方法(for 并行同步)
def oracle_select_insert(sql_info, source_db_info, target_db_info):
    oracle_source = OracleSource(**source_db_info)
    # 判断目标数据库类型，根据不同类型调用不同的插入方法(不同类型数据库的插入方法名保持一致)
    target_db_type = target_db_info.get('target_db_type')
    if target_db_type == 'mysql':
        # 使用时引用，避免交叉引用
        from mysql_migrate import MysqlTarget
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
            sql_select = 'select * from ' + from_table + ' where ' + parallel_key + ' >= ' + str(batch_from) + ' and ' + parallel_key + ' <= ' + str(max_key)
        else:
            sql_select = 'select * from ' + from_table + ' where ' + parallel_key + ' >= ' + str(batch_from) + ' and ' + parallel_key + ' < ' + str(batch_to)
        sql_data = oracle_source.oracle_source_data(sql_select)
        insert_rows = multi_db_target.insert_target_data(to_table, sql_data)
        insert_rows_list.append(insert_rows)
    total_rows = reduce(lambda x, y: x + y, insert_rows_list)
    return total_rows


# 源库是oracle时的并行/串行数据处理
class OracleDataMigrate(object):
    def __init__(self, source_db_info, target_db_info):
        self.source_db_info = source_db_info
        self.target_db_info = target_db_info
        self.oracle_source = OracleSource(**source_db_info)
        # 判断目标数据库类型，根据不同类型调用不同的插入方法(不同类型数据库的插入方法名保持一致)
        self.target_db_type = target_db_info.get('target_db_type')
        if self.target_db_type == 'mysql':
            from mysql_migrate import MysqlTarget
            self.multi_db_target = MysqlTarget(**target_db_info)
        elif self.target_db_type == 'oracle':
            self.multi_db_target = OracleTarget(**target_db_info)
        else:
            pass

    # 源库为oracle时的数据同步串行处理
    def oracle_serial_migrate(self, from_table, to_table):
        print('[DBM] Inserting data into table `' + to_table + '`')
        sql_select = 'select /*+ parallel(t 4) */ * from ' + from_table + ' t'
        # 串行获取数据,每批10w行
        res_data_incr = self.oracle_source.oracle_source_data_incr(sql_select)
        insert_rows_list = []
        while True:
            data_incr = next(res_data_incr)
            if data_incr:
                insert_rows = self.multi_db_target.insert_target_data(to_table, data_incr)
                insert_rows_list.append(insert_rows)
            else:
                break
        total_rows = reduce(lambda x, y: x + y, insert_rows_list) if insert_rows_list else 0
        return total_rows

    # 源库是oracle时的数据同步的并行处理
    def oracle_parallel_migrate(self, from_table, to_table, final_parallel, parallel_key=None, parallel_method='multiprocess'):
        # 分批数据
        from_db = self.source_db_info.get('db')
        to_db = self.target_db_info.get('db')
        sql_max_min = 'select max(' + parallel_key + '), min(' + parallel_key + ') from ' + from_table
        res_max_min = self.oracle_source.oracle_source_data(sql_max_min)
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
                insert_rows = p.map(partial(oracle_select_insert, source_db_info=self.source_db_info, target_db_info=self.target_db_info), sql_select_list)
            total_rows = reduce(lambda x, y: x + y, insert_rows)
            # print(insert_rows)
            # print(total_rows)
        else:
        # 多线程
            thread_list = []
            result_list = []
            for t in range(final_parallel):
                sql_info = sql_select_list[t]
                # sthread = threading.Thread(target=mysql_select_insert, args=(sql_info,self.source_db_info, self.target_db_info), name=('thread-' + str(t)))
                sthread = MyThread(oracle_select_insert, (sql_info, self.source_db_info, self.target_db_info))
                thread_list.append(sthread)
            for t in thread_list:
                t.start()
            for t in thread_list:
                t.join()
            for t in thread_list:
                result_list.append(sthread.get_result())
            total_rows = reduce(lambda x, y: x + y, result_list)
        return total_rows

    # 源库是oracle时的数据同步串行/并行选择
    def oracle_parallel_flag(self, from_table, index_column_info, res_columns, res_segments, parallel=0):
        # 用户指定并发数或行数大于10w或空间使用大于100M，并且unique索引列的数据类型为number且非空时，设置并发
        parallel_flag = 0 if parallel == 0 else 1
        pri_keys = []
        for i in index_column_info:
            pri_col = i.get('column_name') if i.get('non_unique') == 0 else None
            col_list = list(filter(lambda x: x.get('column_name') == pri_col and x.get('nullable') == 'N' and x.get('data_type') == 'NUMBER', res_columns))
            pri_keys.append(col_list[0].get('column_name'))
        if (res_segments[0].get('num_rows') > 100000 or res_segments[0].get('data_length') > 100000000) and pri_keys:
            # 并行主键
            parallel_key = pri_keys[0]
            parallel_flag = 1
        else:
            parallel_key = None
            parallel_flag = 0
        # 设置并行方式（Linux为多进程，windows为多线程） 和最大并发数，这里先全改为多进程
        # parallel_method = 'multiprocess' if os.name == 'posix' else 'threading'
        parallel_method = 'multiprocess'
        max_parallel = cpu_count()
        # 计算实际并发数=用户指定并发数；如未指定，实际并发数=round(num_rows/10w)
        if parallel == 0:
            auto_parallel = round(int(res_segments[0].get('num_rows'))/100000)
            final_parallel = max(min(auto_parallel, max_parallel), 1)
        else:
            final_parallel = min(parallel, max_parallel)
        return parallel_flag, final_parallel, parallel_key, parallel_method


if __name__ == '__main__':
    source_db_info = {'host': '172.20.221.180', 'port': 1522, 'db': 'monitor', 'user': 'emlog',
                      'password': 'emlog123', 'charset': 'utf8', 'source_db_type': 'oracle'}
    oracleconnect = OracleSource(**source_db_info)
    index_column_info = oracleconnect.oracle_source_index('DBM_SYSTIME')
    print(index_column_info)

