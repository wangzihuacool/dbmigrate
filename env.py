# -*- code: utf-8 -*-

import os, sys, re

class EnvLoader:
    def __init__(self):
        self.conf = {}

    '''
    classmethod 修饰符对应的函数不需要实例化，不需要 self 参数，但第一个参数需要是表示自身类的 cls 参数，
    可以来调用类的属性，类的方法，实例化对象等。
    '''
    @classmethod
    def load_with_file(cls, file):
        # cls()即代表自身类
        self = cls()
        conf = {}
        # 读取env.py配置文件的参数，并执行，即在当前类中声明这些参数
        if os.path.exists(file):
            env_content = open(file, encoding='utf8').read()
            exec(env_content, conf)
        return conf


curr_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
config_file = curr_dir + os.sep + "dbmigrate.conf"
conf = EnvLoader.load_with_file(config_file)

# section：源库
source_db_type = conf.get('source_db_type')
source_host = conf.get('source_host')
source_port = conf.get('source_port')
source_db = conf.get('source_db')
source_user = conf.get('source_user')
source_password = conf.get('source_password')
source_tables = conf.get('source_tables')
# section：目标库
target_db_type = conf.get('target_db_type')
target_host = conf.get('target_host')
target_port = conf.get('target_port')
target_db = conf.get('target_db')
target_user = conf.get('target_user')
target_password = conf.get('target_password')
target_tables = conf.get('target_tables')
# section：通用配置
content = conf.get('content')
parallel = conf.get('parallel')
performance_mode = conf.get('performance_mode')
table_exists_action = conf.get('table_exists_action')
incremental_method = conf.get('incremental_method')
where_clause = conf.get('where_clause')
# section: 软件配置
silent_mode = conf.get('silent_mode')