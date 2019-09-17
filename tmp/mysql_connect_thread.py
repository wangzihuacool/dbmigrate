#!/usr/bin/env python3
# -*- codeL: utf-8 -*-
'''
  测试多进程使用pymysql时的插入性能(for linux only,windows的多进程比较坑)
'''
import pymysql
import random
import time
import os
import threading
import traceback


#mysql数据库信息
mysql_host = '172.20.221.194'
mysql_port = 3306
mysql_db = 'sbench'
mysql_user = 'root'
mysql_password = 'root'

#测试SQL语句
SQL_DROP_TABLE = """
drop table if exists `sbtest3`
"""

SQL_CREATE_TABLE = """
CREATE TABLE `sbtest3` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `k` int(11) NOT NULL DEFAULT '0',
  `c` char(120) NOT NULL DEFAULT '',
  `pad` char(60) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8
"""

SQL_INSERT_DATA = """
insert into `sbtest3` (`id`, `k`, `c`, `pad`) values (%s, %s, %s, %s)
"""

SQL_CREATE_INDEX = """
alter table sbtest3 add index idx_k_3 (`k`)
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


#定义方法，子进程连接数据库插入数据
def insert_data(*random_data):
    print('Subprocess ' + str(os.getpid()) + ' start...')
    pause_time_1 = time.time()
    connection = pymysql.connect(host=mysql_host,
                                 port=mysql_port,
                                 db=mysql_db,
                                 user=mysql_user,
                                 password=mysql_password,
                                 charset='utf8mb4'
                                )

    with connection.cursor() as cursor:
        cursor.executemany(SQL_INSERT_DATA, random_data)
    connection.commit()
    cursor.close()
    connection.close()
    pause_time_2 = time.time()
    print("pymysql insert time:" + str(pause_time_2 - pause_time_1))


#连接数据库测试
start_time = time.time()
try:
    connection = pymysql.connect(host=mysql_host,
                                 port=mysql_port,
                                 db=mysql_db,
                                 user=mysql_user,
                                 password=mysql_password,
                                 charset='utf8mb4'
                                )

    with connection.cursor() as cursor:
        cursor.execute(SQL_DROP_TABLE)
        cursor.execute(SQL_CREATE_TABLE)
    cursor.close()

    #初始化4个线程处理数据插入
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

    with connection.cursor() as cursor:
        cursor.execute(SQL_CREATE_INDEX)
    cursor.close()
    connection.close()
except Exception as e:
    traceback.print_exc()
finally:
    stop_time = time.time()
    print("pymysql total time:" + str(stop_time - start_time))
