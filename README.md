
### Lealone是什么

* 是一个兼具RDBMS、NoSQL优点的面向[OLTP](http://en.wikipedia.org/wiki/Online_transaction_processing)场景的分布式关系数据库


### Lealone有哪些特性

* 去中心化集群架构，没有单点故障

* 支持分片(Sharding)、复制

* 强一致性，支持ACID、高性能分布式事务<br>
  使用一种非常新颖的[基于局部时间戳的多版本冲突与有效性检测的分布式事务模型](https://github.com/codefollower/Lealone/blob/master/docs/%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3/%E5%88%86%E5%B8%83%E5%BC%8F%E4%BA%8B%E5%8A%A1%E6%A8%A1%E5%9E%8B.md)
 
* 插件化存储引擎架构，内置[WiredTiger](https://github.com/wiredtiger/wiredtiger/tree/develop)和[MVStore](http://www.h2database.com/html/mvstore.html)存储引擎

* 插件化事务引擎架构，事务处理逻辑与存储分离，内置一个支持MVCC的事务引擎

* SQL语法类似MySQL、PostgreSQL，支持索引、视图、Join、子查询 <br>
  支持触发器、自定义函数、Order By、Group By、聚合

* 从[H2数据库](http://www.h2database.com/html/main.html)和[Apache Cassandra](http://cassandra.apache.org/)借鉴了大量成熟的代码和思想


### Lealone文档

* [文档首页](https://github.com/codefollower/Lealone/blob/master/docs/README.md)


### Lealone名字的由来

* Lealone 发音 ['li:ləʊn] 这是我新造的英文单词， <br>
  灵感来自于办公桌上那些叫绿萝的室内植物，一直想做个项目以它命名。 <br>
  绿萝的拼音是lv luo，与Lealone英文发音有点相同，<br>
  Lealone是lea + lone的组合，反过来念更有意思哦。:)


### Lealone License

* [License](https://github.com/codefollower/Lealone/blob/master/LICENSE.md)

