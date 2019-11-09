1、获取创建数据库脚本
show create database `tpcd`;

2、当大表上不存在主键时，使用cardinality最高的单列索引对应的int类型的列来进行分片，查询前注意先查看执行计划是否能走到索引，查询时使用hint /*!40001 SQL_NO_CACHE */ （done）
查询过程多进程，然后每进程可以再进行循环(done)

3、增加多线程(done)

4、串行执行时如何做（done）

5、主键值是varchar时怎么分批同步 --- 同无主键值，串行处理，分批获取，默认一次批量获取1w行（done）

6、加载参数，打包后仍然可以使用(done)

7、补充mysql的存储过程等不常用模块同步(done)

8、对于行数只有10w行但是主键id的范围很大的情况下的处理优化(done)

9、外键增加 on delete 。。。 on update 。。。操作短句(done)

10、对于drds这种不支持事务的数据库适配