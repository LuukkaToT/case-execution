16 核 Linux 机器反而慢的原因
1. 核数跟这个查询无关
MySQL 一次 SELECT 是单线程执行的，16 核只能让多个并发查询快，但不会让单条 SQL 变快。

2. 公司 MySQL 大概率不在同一台机器
公司环境一般是 app server + db server 分开部署，每次查询有局域网 RTT（哪怕只有 1-2ms）。53 次 ut_case 分页查询 + 53×2 次事务 = 约 160 次网络来回。而你本地是 Docker 直接 localhost，延迟几乎为 0。

3. 最关键：MySQL 执行计划不一样
环境	数据分布	MySQL 选的执行计划
本地
'26B' 占 9%（1/11）
type=index，主键顺序扫，每次只扫 ~1.1万行
公司
'26B' 占比更高（10万行里 52265 条 = 52%）
type=ALL，全表扫描 + filesort
当 '26B' 占比达到 52% 时，MySQL 优化器觉得"这么多行都符合，不如直接全扫一遍再排序"。53 次全表扫描 × 每次约 200ms = 10.6 秒，完全对上了。

本地之所以快，是因为我们插的测试数据 '26B' 只占 9%，MySQL 自动用了更优的方案。

4. 验证一下
在公司机器上跑这条：

EXPLAIN SELECT * FROM `ut_case` WHERE version='26B' ORDER BY case_id LIMIT 1000;
如果 type=ALL，Extra 里有 Using filesort，就坐实了。

结论
不是你电脑 CPU 更好，是 MySQL 执行计划不同。 公司 ut_case 里 '26B' 占比过半，MySQL 选了全表扫描，加上分离部署的网络延迟，才有 9 秒的结果。

加上复合索引 (version, case_id) 之后，无论数据占比是多少，MySQL 都会走 range 扫描，一步定位，几毫秒结束。

