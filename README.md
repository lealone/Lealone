
## Lealone是什么

* 是一个面向[OLTP](http://en.wikipedia.org/wiki/Online_transaction_processing)场景的分布式关系数据库


## Lealone名字的由来

* Lealone 发音 ['li:ləʊn] 这是我新造的英文单词， <br>
  灵感来自于办公桌上那些叫绿萝的室内植物，一直想做个项目以它命名。 <br>
  绿萝的拼音是lv luo，与Lealone英文发音有点相同，<br>
  Lealone是lea + lone的组合，反过来念更有意思哦。:)


## Lealone有哪些特性

* 去中心化集群架构，没有单点故障

* 支持分片(Sharding)、复制

* 强一制性，支持ACID、高性能分布式事务<br>
  使用一种非常新颖的[基于局部时间戳的多版本冲突与有效性检测的分布式事务模型](https://github.com/codefollower/Lealone/wiki/Lealone-transaction-model)
 
* 插件化存储引擎架构，内置[WiredTiger](https://github.com/wiredtiger/wiredtiger/tree/develop)和[MVStore](http://www.h2database.com/html/mvstore.html)存储引擎

* SQL语法类似MySQL、PostgreSQL，支持索引、视图、Join、子查询 <br>
  支持触发器、自定义函数、Order By、Group By、聚合

* 从[H2数据库](http://www.h2database.com/html/main.html)和[Apache Cassandra](http://cassandra.apache.org/)借鉴了大量成熟的代码和思想


## Lealone文档

* [用户文档](https://github.com/codefollower/Lealone/wiki/Lealone%E7%94%A8%E6%88%B7%E6%96%87%E6%A1%A3)

* [设计文档](https://github.com/codefollower/Lealone/wiki/Lealone%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)

* [开发文档](https://github.com/codefollower/Lealone/wiki/Lealone%E5%BC%80%E5%8F%91%E6%96%87%E6%A1%A3)


## License

* [License](https://github.com/codefollower/Lealone/blob/master/LICENSE.md)

