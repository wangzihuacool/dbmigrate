# -*- code: utf-8 -*-

from_db = 'sbench'
from_table = 'sbtest1'
parallel_key = 'id'
min_key = 3
per_rows = 200000
sql_list = []
for p in range(4):
    print(p)
    sql_select = 'select * from `' + from_db + '`.`' + from_table + '` where `' + parallel_key + '` >= ' + str(
        min_key + (p * per_rows)) + ' and `' + parallel_key + '` < ' + str(min_key + ((p+1) * per_rows))
    sql_list.append(sql_select)
print(sql_list)