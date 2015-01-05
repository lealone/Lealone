
## Lealone是什么

* 既是一个可用于[HBase](http://hbase.apache.org/)、[Cassandra](http://cassandra.apache.org/)、[MySQL](http://www.mysql.com/)、[PostgreSQL](http://www.postgresql.org/)的分布式SQL与分布式事务引擎

* 也是一个可以独立运行的分布式数据库

## Lealone名字的由来

* Lealone 发音 ['li:ləʊn] 这是我新造的英文单词， <br>
  灵感来自于办公桌上那些叫绿萝的室内植物，一直想做个项目以它命名。 <br>
  绿萝的拼音是lv luo，与Lealone英文发音有点相同，<br>
  Lealone是lea + lone的组合，反过来念更有意思哦。:)


## Lealone有哪些特性

* 支持高性能的分布式事务，<br>
  使用一种非常新颖的[基于局部时间戳的多版本冲突与有效性检测的分布式事务模型](https://github.com/codefollower/Lealone/wiki/Lealone-transaction-model)

* 对[H2数据库](http://www.h2database.com/html/main.html)的SQL引擎进行了大量的改进和扩展

* 支持MySQL、PostgreSQL的SQL语法

* 支持JDBC 4.0规范

* 支持索引、视图、Join、子查询、各种DDL <br>
  支持触发器、自定义函数、Order By、Group By、聚合


## Lealone用户文档

* [HBase用户文档](https://github.com/codefollower/Lealone/wiki/HBase%E7%94%A8%E6%88%B7%E6%96%87%E6%A1%A3)

* [Cassandra用户文档](https://github.com/codefollower/Lealone/wiki/Cassandra%E7%94%A8%E6%88%B7%E6%96%87%E6%A1%A3)

* [CBase用户文档](https://github.com/codefollower/Lealone/wiki/CBase%E7%94%A8%E6%88%B7%E6%96%87%E6%A1%A3)

* MySQL用户文档

* PostgreSQL用户文档


## Lealone开发者文档

* [Lealone开发者文档](https://github.com/codefollower/Lealone/wiki/Lealone%E5%BC%80%E5%8F%91%E8%80%85%E6%96%87%E6%A1%A3)


## Lealone 1.0 Roadmap

* 继续优化join、subquery、view、index的性能
* 支持Cassandra、MySQL、PostgreSQL
* 新的CBase存储引擎

## License

以下子工程中的代码使用[H2数据库的License](http://www.h2database.com/html/license.html)
* lealone-client
* lealone-common
* lealone-server
* lealone-sql
* lealone-storage-cbase

除此之外的代码使用[Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
