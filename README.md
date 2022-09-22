
### Lealone 是什么

* 是一个兼具 RDBMS、NoSQL 优点的面向微服务和 OLTP/OLAP 场景的单机与分布式关系数据库


### Lealone 有哪些特性

##### 高亮特性

* 全链路异步化，使用少量线程就能处理大量并发，内置新颖的异步化 B-Tree

* 可暂停的、渐进式的 SQL 引擎

* 基于 SQL 优先级的抢占式调度，慢查询不会长期霸占 CPU

* 创建 JDBC 连接非常快速，占用资源少，不再需要 JDBC 连接池
 
* 插件化存储引擎架构，内置 AOSE 引擎

* 插件化事务引擎架构，事务处理逻辑与存储分离，内置 AOTE 引擎

* 支持 Page 级别的行列混合存储，对于有很多字段的表，只读少量字段时能大量节约内存

* 支持通过 CREATE SERVICE 创建可托管的后端服务

* 内置类型安全的 ORM/DSL 框架，不需要配置文件和注解

* 非常小的绿色环保安装包，只有 2M 左右的大小


##### 普通特性

* 支持索引、视图、Join、子查询、触发器、自定义函数、Order By、Group By、聚合


##### 企业版

* 支持高性能分布式事务、支持强一致性复制、支持全局快照隔离

* 支持自动化分片 (Sharding)，用户不需要关心任何分片的规则，没有热点，能够进行范围查询

* 支持混合运行模式，包括4种模式: 嵌入式、Client/Server 模式、复制模式、Sharding 模式

* 支持不停机快速手动或自动转换运行模式: Client/Server 模式 -> 复制模式 -> Sharding 模式

* 支持表达式编译、支持向量化


### Lealone 文档

* [快速入门](https://github.com/lealone/Lealone-Docs/blob/master/%E5%BA%94%E7%94%A8%E6%96%87%E6%A1%A3/%E7%94%A8%E6%88%B7%E6%96%87%E6%A1%A3.md)

* [文档首页](https://github.com/lealone/Lealone-Docs)


### Lealone 微服务框架

* 非常新颖的基于数据库技术实现的微服务框架，开发分布式微服务应用跟开发单体应用一样简单

* [使用 Lealone 开发单机和分布式微服务应用](https://github.com/lealone/Lealone-Docs/blob/master/%E5%BA%94%E7%94%A8%E6%96%87%E6%A1%A3/%E4%BD%BF%E7%94%A8Lealone%E5%BC%80%E5%8F%91%E5%8D%95%E6%9C%BA%E5%92%8C%E5%88%86%E5%B8%83%E5%BC%8F%E5%BE%AE%E6%9C%8D%E5%8A%A1%E5%BA%94%E7%94%A8.md)

* [使用 JavaScript 和 Python 语言开发微服务应用](https://github.com/lealone/Lealone-Polyglot)

* [文档与演示例子](https://github.com/lealone/Lealone-Examples)


### Lealone ORM 框架

* 超简洁的类型安全的 ORM 框架

* [Lealone ORM 框架快速入门](https://github.com/lealone/Lealone-Docs/blob/master/%E5%BA%94%E7%94%A8%E6%96%87%E6%A1%A3/Lealone%20ORM%E6%A1%86%E6%9E%B6%E5%BF%AB%E9%80%9F%E5%85%A5%E9%97%A8.md)


### Lealone xSQL

* [使用 MySQL 或 PostgreSQL 的客户端和 SQL 语法访问 Lealone 数据库](https://github.com/lealone/Lealone-Docs/blob/master/%E5%BA%94%E7%94%A8%E6%96%87%E6%A1%A3/%E4%BD%BF%E7%94%A8MySQL%E6%88%96PostgreSQL%E7%9A%84%E5%AE%A2%E6%88%B7%E7%AB%AF%E5%92%8CSQL%E8%AF%AD%E6%B3%95%E8%AE%BF%E9%97%AELealone%E6%95%B0%E6%8D%AE%E5%BA%93.md)

* [更多细节...](https://github.com/lealone/Lealone-xSQL)


### Lealone QinSQL

* 数据湖仓 OLTP 一体化平台

* [更多细节...](https://github.com/lealone/QinSQL)


### Lealone Web 客户端

* [Ops Center](https://github.com/lealone/Lealone-OpsCenter)


### Lealone Plugins

* 支持可插拨的存储引擎 (MemoryStore、MVStore)

* [更多细节...](https://github.com/lealone/Lealone-Plugins)


### Lealone 名字的由来

* Lealone 发音 ['li:ləʊn] 这是我新造的英文单词， <br>
  灵感来自于办公桌上那些叫绿萝的室内植物，一直想做个项目以它命名。 <br>
  绿萝的拼音是 lv luo，与 Lealone 英文发音有点相同，<br>
  Lealone 是 lea + lone 的组合，反过来念更有意思哦。:)


### Lealone 历史

* 2012年从 [H2 数据库 ](http://www.h2database.com/html/main.html)的代码开始

* 为了支持 lealone-p2p 集群改编了 [Cassandra](https://cassandra.apache.org/) 的一些代码

* [Lealone 的过去现在将来](https://github.com/codefollower/My-Blog/issues/16)


### Lealone License

* [License](https://github.com/lealone/Lealone/blob/master/LICENSE.md)
