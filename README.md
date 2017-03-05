
### Lealone是什么

* 是一个兼具RDBMS、NoSQL优点的面向[OLTP](http://en.wikipedia.org/wiki/Online_transaction_processing)场景的异步化NewSQL单机与分布式关系数据库


### Lealone有哪些特性

##### 开源版本(不支持分布式事务)

* 完全异步化，使用少量线程就能处理大量并发

* 基于SQL优先级的抢占式调度，慢查询不会长期霸占CPU

* 创建JDBC连接非常快速，占用资源少，不再需要JDBC连接池
 
* 插件化存储引擎架构，内置AOSE引擎，支持单机与分布式存储

* 插件化事务引擎架构，事务处理逻辑与存储分离，内置MVCC引擎，支持单机事务

* 支持自动化分片(Sharding)，用户不需要关心任何分片的规则，没有热点，能够进行范围查询

* 支持索引、视图、Join、子查询、触发器、自定义函数、Order By、Group By、聚合

* 从[H2数据库](http://www.h2database.com/html/main.html)借鉴了大量成熟的代码和思想


##### 企业版本

* 支持[分布式事务](https://github.com/codefollower/Lealone/blob/master/docs/%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3/%E5%88%86%E5%B8%83%E5%BC%8F%E4%BA%8B%E5%8A%A1%E6%A8%A1%E5%9E%8B.md)

* 支持[全局快照隔离](https://github.com/codefollower/My-Blog/issues/8)

* 支持强一致性复制



### Lealone文档

* [Getting started](https://github.com/codefollower/Lealone/blob/master/docs/%E5%BA%94%E7%94%A8%E6%96%87%E6%A1%A3/%E7%94%A8%E6%88%B7%E6%96%87%E6%A1%A3.md#1-%E5%BF%AB%E9%80%9F%E5%85%A5%E9%97%A8)

* [文档首页](https://github.com/codefollower/Lealone/blob/master/docs/README.md)


### Lealone名字的由来

* Lealone 发音 ['li:ləʊn] 这是我新造的英文单词， <br>
  灵感来自于办公桌上那些叫绿萝的室内植物，一直想做个项目以它命名。 <br>
  绿萝的拼音是lv luo，与Lealone英文发音有点相同，<br>
  Lealone是lea + lone的组合，反过来念更有意思哦。:)


### Lealone License

* [License](https://github.com/codefollower/Lealone/blob/master/LICENSE.md)

