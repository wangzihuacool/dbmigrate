# dbmigrate - 关系型数据库同步工具帮助文档

## 简介

   dbmigrate是一个易于使用的高性能的关系型数据库同步工具，支持Mysql、Oracle、Mssql数据库之间的同构/异构数据迁移，未来计划支持包括Postgres在内的任意关系型数据库之间元数据/数据同步。目前Mysql相关的功能最为丰富，支持库级别、表级别的元数据/数据同步，支持全库索引同步，支持表级别的自定义过滤条件增量同步，支持表级别/行级别的并行同步。Oracle和Mysql、Mssql之间的异构复制目前支持表级别同步。

## 开始使用

下载dbmigrate和dbmigrate.conf.sample到迁移服务器的同一目录，dbmigrate.conf.sample更名为dbmigrate.conf，修改配置文件，执行dbmigrate即可。

下载地址：

- http://172.20.222.218:9003/dbmigrate/dbmigrate
- http://172.20.222.218:9003/dbmigrate/dbmigrate.conf



## 参数介绍

```
source_db_type            - 源库类型(目前支持oracle/mysql/mssql)

source_host               - 源库IP地址

source_port               - 源库端口，默认3306

source_db                 - 源库数据库名

source_user               - 源库用户

source_password           - 源库用户密码

source_tables             - 库中待同步的表，以列表方式存储，当列表为空时，同步源库整个库到目标库

target_db_type            - 目标库类型(目前支持oracle/mysql)

target_host               - 目标库IP地址

target_port               - 目标库端口

target_db                 - 目标库数据库名，目标库名可以与源库不一样；当target_db为空时，则在目标库新建一个与源库同名的数据库

target_user               - 目标库用户,mysql数据库级复制时确保该用户拥有对应的权限

target_password           - 目标库用户密码

target_tables             - 目标表，目标表目前不支持手动指定名称，默认为None，即跟source_tables一致

content                   - 同步内容: [all] - 同步元数据+数据，[metadata] - 只同步元数据，[data] - 只同步数据, [index] - 只同步索引, [increment] - 增量同步数据

parallel                  - 指定并行度，默认为0即由程序自行判断并行度，建议值：0

performance_mode          - 是否开启高性能模式，可选项：0-关闭，默认值,建议值；1-打开，自动判断表级别并行度，开启后会消耗更多的系统资源；n-大于1的值，手动指定表级别并行度，慎选

table_exists_action       - 目标表在目标库已存在时的处理方式：drop - 删除旧表；truncate - truncate表数据；append - 插入数据；skip - 跳过该表

incremental_method        - 获取增量数据的方式，[where] - 自定义where查询条件，仅content=increment时生效

where_clause              - 获取增量数据时的查询条件,仅当content='increment'和incremental_method='where'时生效

auto_upgrade              - 是否自动升级版本,建议值：1，自动更新以及时修复bug和增强功能；仅dbmigrate作为后台任务执行时可设置为0
```

## 配置示例：

[mysql全库同步]

```
#源库
source_db_type = 'mysql'
source_host = '172.16.1.1'
source_port = 3306
source_db = 'testdb'
source_user = 'test'
source_password = 'test'
source_tables = []

#目标库
target_db_type = 'mysql'
target_host = '172.16.1.2'
target_port = 3306
target_db = 'testdb_2'                                                    
target_user = 'test'
target_password = 'test'
target_tables = None                                                  

#通用配置
content = 'all'                                                  
parallel = 0                                                          
performance_mode = 0                                                  
table_exists_action = 'drop'                                          
incremental_method = 'where'                                          
where_clause = 'where create_time >= date_sub(curdate(), interval 2 day)'   

#软件配置
auto_upgrade = 1                                                      
```

