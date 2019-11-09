import pymysql

def mysql_select_incr():
    connection = pymysql.connect(host='172.20.xx.xx',
                                 port=3306,
                                 db='sbench',
                                 user='test',
                                 password='test',
                                 charset='utf8')

    sql = 'select * from test2'
    with connection.cursor() as conn_cursor:
        conn_cursor.arraysize = 3  # 设置一次批量获取的行数
        conn_cursor.execute(sql)
        while True:
            results = conn_cursor.fetchmany()
            yield results
            if not results:
                break

res = mysql_select_incr()
while True:
    a = next(res)
    if not a:
        break
    print(a)


