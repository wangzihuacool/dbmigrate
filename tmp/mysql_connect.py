# -*- codeL: utf-8 -*-
'''
  测试pymysql未使用连接池时的插入性能
'''
import pymysql
import random
import time
import traceback

#mysql数据库信息
mysql_host = '172.20.xx.xx'
mysql_port = 3306
mysql_db = 'sbench'
mysql_user = 'test'
mysql_password = 'test'

#测试SQL语句
SQL_DROP_TABLE = """
drop table if exists `sbtest2`
"""

SQL_CREATE_TABLE = """
CREATE TABLE `sbtest2` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `k` int(11) NOT NULL DEFAULT '0',
  `c` char(120) NOT NULL DEFAULT '',
  `pad` char(60) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8
"""

SQL_INSERT_DATA = """
insert into `sbtest2` (`id`, `k`, `c`, `pad`) values (%s, %s, %s, %s)
"""

SQL_CREATE_INDEX = """
alter table sbtest2 add index idx_k (`k`)
"""

#生成随机数
random_data = ((None, random.randint(0,99999999), '68487932199-96439406143-93774651418-41631865787-96406072701-20604855487-25459966574-28203206787-41238978918-19503783441', '22195207048-70116052123-74140395089-76317954521-98694025897') for i in range(1000000))

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
        cursor.executemany(SQL_INSERT_DATA, random_data)
    connection.commit()
    cursor.close()
    pause_time = time.time()
    print("pymysql insert time:" + str(pause_time - start_time))
    with connection.cursor() as cursor:
        cursor.execute(SQL_CREATE_INDEX)
    cursor.close()
    connection.close()
except Exception as e:
    traceback.print_exc()
finally:
    stop_time = time.time()
    print("pymysql total time:" + str(stop_time - start_time))
