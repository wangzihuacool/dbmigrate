#!/usr/bin/env python3
# -*- codeL: utf-8 -*-
'''
  测试多进程使用pymysql时的插入性能(for linux only,windows的多进程比较坑)
'''
import pymysql
import random
import time
import os
from multiprocessing import Process
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

    #初始化4个子进程处理数据插入
    print('Child process start.')
    p1 = Process(target=insert_data, args=(*random_data_1,))
    p2 = Process(target=insert_data, args=(*random_data_2,))
    p3 = Process(target=insert_data, args=(*random_data_3,))
    p4 = Process(target=insert_data, args=(*random_data_4,))
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p1.join()
    p2.join()
    p3.join()
    p4.join()
    print('Child process stop.')

    with connection.cursor() as cursor:
        cursor.execute(SQL_CREATE_INDEX)
    cursor.close()
    connection.close()
except Exception as e:
    traceback.print_exc()
finally:
    stop_time = time.time()
    print("pymysql total time:" + str(stop_time - start_time))
