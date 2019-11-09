#!/usr/bin/env python3
# -*- codeL: utf-8 -*-
'''
  测试多线程使用pymysql_pool连接池时的插入性能
'''
import pymysqlpool
import random
import time
import os
import threading
import traceback


#mysql数据库信息
mysql_host = '172.20.xx.xx'
mysql_port = 3306
mysql_db = 'sbench'
mysql_user = 'test'
mysql_password = 'test'

#测试SQL语句
SQL_DROP_TABLE = """
drop table if exists `sbtest4`
"""

SQL_CREATE_TABLE = """
CREATE TABLE `sbtest4` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `k` int(11) NOT NULL DEFAULT '0',
  `c` char(120) NOT NULL DEFAULT '',
  `pad` char(60) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8
"""

SQL_INSERT_DATA = """
insert into `sbtest4` (`id`, `k`, `c`, `pad`) values (%s, %s, %s, %s)
"""

SQL_CREATE_INDEX = """
alter table sbtest4 add index idx_k_4 (`k`)
"""

# 生成随机数
random_data_1 = ((None, random.randint(0, 99999999),
                '68487932199-96439406143-93774651418-41631865787-96406072701-20604855487-25459966574-28203206787-41238978918-19503783441',
                '22195207048-70116052123-74140395089-76317954521-98694025897') for i in range(250000))
random_data_2 = ((None, random.randint(0, 99999999),
                '68487932199-96439406143-93774651418-41631865787-96406072701-20604855487-25459966574-28203206787-41238978918-19503783441',
                '22195207048-70116052123-74140395089-76317954521-98694025897') for i in range(250000))
random_data_3 = ((None, random.randint(0, 99999999),
                '68487932199-96439406143-93774651418-41631865787-96406072701-20604855487-25459966574-28203206787-41238978918-19503783441',
                '22195207048-70116052123-74140395089-76317954521-98694025897') for i in range(250000))
random_data_4 = ((None, random.randint(0, 99999999),
                '68487932199-96439406143-93774651418-41631865787-96406072701-20604855487-25459966574-28203206787-41238978918-19503783441',
                '22195207048-70116052123-74140395089-76317954521-98694025897') for i in range(250000))

#定义方法，子线程从连接池获取连接插入数据
def insert_data(*random_data):
    print(threading.current_thread().name + ' start...')
    pause_time_1 = time.time()
    #print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    if pool1.pool_size() <= 0:
        print('Warning: No available connection in connection pool!')
        exit(1)
    else:
        conn_new = pool1.get_connection()
        #print('Info: pool1 now available connections: ' + str(pool1.pool_size()))
        with conn_new as cursor:
            cursor.executemany(SQL_INSERT_DATA, random_data)
        conn_new.commit()
        conn_new.close()
        pause_time_2 = time.time()
        print(threading.current_thread().name + ' pymysql insert time:' + str(pause_time_2 - pause_time_1))
        #print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))



#连接数据库测试
start_time = time.time()
try:
    config = {'host': mysql_host, 'user': mysql_user, 'password': mysql_password, 'database': mysql_db, 'autocommit': True}
    ### 创建一个最大连接为2的连接池
    #pymysqlpool.logger.setLevel('INFO')
    pool1 = pymysqlpool.ConnectionPool(size=4, name='pool1', **config)
    #print('pool1 size :' + str(pool1.pool_size()))
    # 获取1个连接用来创建表
    conn1 = pool1.get_connection()
    with conn1 as cursor:
        try:
            cursor.execute(SQL_DROP_TABLE)
        except Exception as e:
            traceback.print_exc()
        cursor.execute(SQL_CREATE_TABLE)
    conn1.close()

    #初始化2个线程处理数据插入
    print('Child thread start.')
    t1 = threading.Thread(target=insert_data, args=(*random_data_1,), name='Thread-1')
    t2 = threading.Thread(target=insert_data, args=(*random_data_2,), name='Thread-2')
    t3 = threading.Thread(target=insert_data, args=(*random_data_3,), name='Thread-3')
    t4 = threading.Thread(target=insert_data, args=(*random_data_4,), name='Thread-4')
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    print('Child thread stop.')

    #最后获取1个连接创建索引
    conn2 = pool1.get_connection()
    #print('Info: pool1 now available connections: ' + str(pool1.pool_size()))
    with conn2 as cursor:
        cursor.execute(SQL_CREATE_INDEX)
    conn2.close()
except Exception as e:
    traceback.print_exc()
finally:
    stop_time = time.time()
    print("pymysql total time:" + str(stop_time - start_time))
