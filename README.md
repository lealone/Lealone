## Lealone名字的由来?

Lealone 发音 ['li:ləʊn]
这是我新造的英文单词，灵感来自于在淘宝工作期间办公桌上那些叫绿萝的室内植物，一直想做个项目以它命名。 
绿萝的拼音是lv luo，与Lealone英文发音有点相同，
Lealone是lea + lone的组合(lea 草地/草原, lone 孤独的)，也算是现在的心境：思路辽阔但又孤独。
反过来念更有意思。

## Lealone是什么?

* 是一个可用于HBase的分布式SQL引擎

* 是对[H2关系数据库](http://www.h2database.com/html/main.html)的改进和扩展

* 是一个100%纯Java的、将BigTable和RDBMS融合的数据库


## 有哪些应用场景?

* 使用Lealone的分布式SQL引擎，可使用类似MySQL的SQL语法和标准JDBC API读写HBase中的数据，
  支持分布式事务、索引、各种DDL，支持触发器、自定义函数、视图、Join、子查询、Order By、Group By、聚合。

* 对于Client/Server架构的传统单机RDBMS的场景，也可使用Lealone。

* 如果应用想不经过网络直接读写数据库，可使用嵌入式Lealone。


## 运行需要

* HBase 0.94.2 或更高 (0.96未测试)
* JDK 6 或更高 (JDK 7未测试)


## 构建需要

* HBase 0.94.2 或更高 (0.96未测试)
* JDK 6 或更高 (JDK 7未测试)
* Maven 2或更高


## 安装配置

* [Lealone安装配置](https://github.com/codefollower/Lealone/wiki/Lealone%E5%AE%89%E8%A3%85%E9%85%8D%E7%BD%AE)


## 快速入门
* [Lealone快速入门](https://github.com/codefollower/Lealone/wiki/Lealone%E5%BF%AB%E9%80%9F%E5%85%A5%E9%97%A8)


## 开发测试环境搭建

* [Lealone开发测试环境搭建](https://github.com/codefollower/Lealone/wiki/Lealone%E5%BC%80%E5%8F%91%E6%B5%8B%E8%AF%95%E7%8E%AF%E5%A2%83%E6%90%AD%E5%BB%BA)


## 使用文档

* [Lealone使用文档](https://github.com/codefollower/Lealone/wiki/Lealone%E4%BD%BF%E7%94%A8%E6%96%87%E6%A1%A3)


## 开发文档

* [Lealone开发文档](https://github.com/codefollower/Lealone/wiki/Lealone%E5%BC%80%E5%8F%91%E6%96%87%E6%A1%A3)


## Roadmap

* Join、子查询、索引性能优化

* OLAP存储引擎


## Package

mvn clean package assembly:assembly -Dmaven.test.skip=true

