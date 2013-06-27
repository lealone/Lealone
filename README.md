## Lealone名字的由来

Lealone 发音 ['li:ləʊn]
这是我新造的英文单词，灵感来自于在淘宝工作期间办公桌上那些叫绿萝的室内植物，一直想做个项目以它命名。 
绿萝的拼音是lv luo，与Lealone英文发音有点相同，
Lealone是lea + lone的组合(lea 草地/草原, lone 孤独的)，也算是现在的心境：思路辽阔但又孤独。
反过来念更有意思。

## Lealone是什么

* 是一个可用于HBase的分布式SQL引擎

* 支持高性能的分布式事务，使用一个非常新颖的 **基于局部时间戳的多版本冲突与有效性检测的事务模型**

* 是对[H2关系数据库](http://www.h2database.com/html/main.html)SQL引擎的改进和扩展



## 有哪些特性

* 支持MySQL、PostgreSQL的SQL语法

* 支持JDBC 4.0规范

* 支持分布式事务、索引、各种DDL，支持触发器、自定义函数、视图、Join、子查询、Order By、Group By、聚合


## 运行需要

* HBase 0.94.2 或更高 (只支持0.94系列版本)
* JDK 6 或 JDK 7


## 构建需要

* HBase 0.94.2 或更高 (只支持0.94系列版本)
* JDK 6 或 JDK 7
* Maven 2或更高


## 安装配置

* [Lealone安装配置](https://github.com/codefollower/Lealone/wiki/Lealone%E5%AE%89%E8%A3%85%E9%85%8D%E7%BD%AE)


## 快速入门

* [Lealone快速入门](https://github.com/codefollower/Lealone/wiki/Lealone%E5%BF%AB%E9%80%9F%E5%85%A5%E9%97%A8)


## 使用Python访问Lealone

* [使用Python访问Lealone](https://github.com/codefollower/Lealone/wiki/%E4%BD%BF%E7%94%A8Python%E8%AE%BF%E9%97%AELealone)


## 开发测试环境搭建

* [Lealone开发测试环境搭建](https://github.com/codefollower/Lealone/wiki/Lealone%E5%BC%80%E5%8F%91%E6%B5%8B%E8%AF%95%E7%8E%AF%E5%A2%83%E6%90%AD%E5%BB%BA)


## 使用文档

* [Lealone使用文档](https://github.com/codefollower/Lealone/wiki/Lealone%E4%BD%BF%E7%94%A8%E6%96%87%E6%A1%A3)


## 开发文档

* [Lealone开发文档](https://github.com/codefollower/Lealone/wiki/Lealone%E5%BC%80%E5%8F%91%E6%96%87%E6%A1%A3)


## Roadmap

* 继续优化join、subquery、view、index的性能
* 支持Cassandra

## Package

mvn clean package assembly:assembly -Dmaven.test.skip=true


## License

下面4个子工程中的代码使用[H2数据库的License](http://www.h2database.com/html/license.html)
* lealone-client
* lealone-mvstore
* lealone-pagestore
* lealone-sql

除此之外的代码使用[Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
