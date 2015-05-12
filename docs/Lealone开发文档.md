
### 1. 开发环境

* Eclipse 3.8.2+
* JDK 1.7+
* Maven 2或3

### 2. 下载项目源代码

git clone https://github.com/codefollower/Lealone.git

### 3. 把Lealone的代码导入Eclipse

如果在eclipse中安装了maven插件，比如m2eclipse，直接在eclipse中导入Lealone的maven工程即可；<br>
否则的话，可以用命令行的方式: <br>
mvn eclipse:eclipse <br>
此命令会根据pom.xml生成一个eclipse工程项目，生成的文件放在与pom.xml平级的目录中，<br>
然后在eclipse中选File->Import->General->Existing Projects into Workspace，<br>
最后点Browse按钮找到刚才生成的eclipse工程项目。<br>

### 4. 启动Lealone并运行测试用例

Lealone有三种运行模式：embedded、client_server、cluster <br>
分别对应lealone-test子工程org.lealone.test.start包的3个子目录，里面有对应的启动程序，<br>
比如以client_server模式来运行测试用例时，先运行TcpServerStart，<br>
提示下面这行信息时就表示启动成功了:<br>
`Lealone TCP Server started, listening address: localhost, port: 5210`

<br>
然后在Eclipse中右击lealone-test子工程名，点Run As -> JUnit Test就可以跑所有测试用例了。


### 5. Lealone的代码风格

在Eclipse中点Window->Preferences->Java->Code Style->Formatter->Import,

把lealone.code.style.xml文件导入进来，
提交代码时需要格式化，缩进用4个空格，文本文件编码使用UTF-8。

### 6. Lealone源代码的目录结构


* lealone-common

  公共代码，例如一些工具类


* lealone-client

  客户端代码，实现了JDBC 4.0规范的常用功能


* lealone-sql 

  SQL引擎的代码


* lealone-mvdb 

  包括MVTable、MVIndex这样的公用模式对象


* lealone-transaction

  事务引擎的代码

  
* lealone-server 

  TCP server接收client端发来的请求，实现client和server之间的传输协议


* lealone-cluster

  集群相关的代码


* lealone-bootstrap

  用来启动lealone


* lealone-storage-mvstore

  MVStore存储引擎的代码


* lealone-storage-wiredtiger

  WiredTiger存储引擎的客户端Java API


* lealone-storage-engine

  为MVStore和WiredTiger实现所有与StorageEngine相关的API


* lealone-test

  所有测试用例的代码


### 7. 各模块的依赖关系


```
bootstrap -> cluster -> server -> sql -> client -> common

storage-engine
  -> transaction -> common
  -> mvdb -> sql
  -> storage-mvstore -> common
  -> storage-wiredtiger

test
  -> bootstrap
  -> storage-engine
```