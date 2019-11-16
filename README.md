
### Lealone是什么
* 是一个兼具RDBMS、NoSQL优点的面向[OLTP](http://en.wikipedia.org/wiki/Online_transaction_processing)场景的异步化NewSQL单机与分布式关系数据库


### Lealone有哪些特性

##### 高亮特性

* 支持高性能分布式事务、支持强一致性复制、支持全局快照隔离

* 完全异步化，使用少量线程就能处理大量并发

* 基于SQL优先级的抢占式调度，慢查询不会长期霸占CPU

* 创建JDBC连接非常快速，占用资源少，不再需要JDBC连接池
 
* 插件化存储引擎架构，内置AOSE引擎，支持单机与分布式存储

* 插件化事务引擎架构，事务处理逻辑与存储分离，内置MVCC引擎，支持单机事务

* 支持列锁，不同事务对同一行记录的不同列进行更新时，不会发生冲突

* 支持Page级别的行列混合存储，对于有很多字段的表，只读少量字段时能大量节约内存

* 支持自动化分片(Sharding)，用户不需要关心任何分片的规则，没有热点，能够进行范围查询

* 支持混合运行模式，包括4种模式: 嵌入式、Client/Server模式、复制模式、Sharding模式

* 支持不停机快速手动或自动转换运行模式(Client/Server模式 -> 复制模式 -> Sharding模式)

* 支持通过CREATE SERVICE创建可托管的后端服务

* 支持异步化的lock free b-tree

* 非常小的绿色环保安装包，只有2M左右的大小


##### 普通特性

* 支持索引、视图、Join、子查询、触发器、自定义函数、Order By、Group By、聚合

* 从[H2数据库](http://www.h2database.com/html/main.html)借鉴了大量成熟的代码和思想



### Lealone文档

* [快速入门](https://github.com/lealone/Lealone-Docs/blob/master/%E5%BA%94%E7%94%A8%E6%96%87%E6%A1%A3/%E7%94%A8%E6%88%B7%E6%96%87%E6%A1%A3.md)

* [文档首页](https://github.com/lealone/Lealone-Docs)

### Lealone Plugins

* 支持可插拨的网络应用框架(Apache MINA、Netty、Vert.x)

* 支持可插拨的存储引擎(WiredTiger、MVStore、RocksDB)

* 初步支持PostgreSQL协议

* 初步支持MySQL协议和语法

* [更多细节...](https://github.com/lealone/Lealone-Plugins)

### Lealone ORM/DSL

* 超简洁的类型安全的ORM/DSL框架

* [更多细节...](https://github.com/lealone/Lealone-Plugins/tree/master/lealone-plugins-test/src/test/java/org/lealone/plugins/test/orm)

### Lealone名字的由来

* Lealone 发音 ['li:ləʊn] 这是我新造的英文单词， <br>
  灵感来自于办公桌上那些叫绿萝的室内植物，一直想做个项目以它命名。 <br>
  绿萝的拼音是lv luo，与Lealone英文发音有点相同，<br>
  Lealone是lea + lone的组合，反过来念更有意思哦。:)


### Lealone License

* [License](https://github.com/lealone/Lealone/blob/master/LICENSE.md)

