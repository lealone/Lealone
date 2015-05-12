# 目录

1. [快速入门](https://github.com/codefollower/Lealone/wiki/Lealone%E7%94%A8%E6%88%B7%E6%96%87%E6%A1%A3#1-%E5%BF%AB%E9%80%9F%E5%85%A5%E9%97%A8)

2. [搭建Lealone集群](https://github.com/codefollower/Lealone/wiki/Lealone%E7%94%A8%E6%88%B7%E6%96%87%E6%A1%A3#2-%E6%90%AD%E5%BB%BAlealone%E9%9B%86%E7%BE%A4)

3. [SQL Reference](https://github.com/codefollower/Lealone/wiki/Lealone%E7%94%A8%E6%88%B7%E6%96%87%E6%A1%A3#3-sql-reference)

4. [使用Python客户端](https://github.com/codefollower/Lealone/wiki/Lealone%E7%94%A8%E6%88%B7%E6%96%87%E6%A1%A3#4-%E4%BD%BF%E7%94%A8python%E5%AE%A2%E6%88%B7%E7%AB%AF)

5. [使用PostgreSQL客户端](https://github.com/codefollower/Lealone/wiki/Lealone%E7%94%A8%E6%88%B7%E6%96%87%E6%A1%A3#5-%E4%BD%BF%E7%94%A8postgresql%E5%AE%A2%E6%88%B7%E7%AB%AF)

# 1. 快速入门

### 1.1. 运行环境

* JDK 1.7+
* Maven 2或3

### 1.2. 下载项目源代码

`git clone https://github.com/codefollower/Lealone.git`
<p>假设源代码放在**E:\lealone**

### 1.3. 从源代码构建

进入**E:\lealone**目录，运行: `mvn clean package assembly:assembly -Dmaven.test.skip=true`
<p>生成的文件放在**E:\lealone\target**目录中， <br>
默认生成**lealone-x.y.z.tar.gz**和**lealone-x.y.z.zip**两个压缩文件，<br>
其中x.y.z代表实际的版本号，下文假设把这两个文件中的任意一个解压到**E:\lealone-1.0.0**

### 1.4. 启动Lealone (单机client-server模式)

进入**E:\lealone-1.0.0\bin**目录，运行: `lealone.bat`
<p>如果输出信息中看到`TcpServer started`表示启动成功了。
<p>要停止Lealone，直接按Ctrl + C

### 1.5. CRUD Example

只需要懂JDBC和SQL就可以轻松看懂下面的代码:

```
import java.sql.*;

public class CRUDExample {
	public static void main(String[] args) throws Exception {
		String url = "jdbc:lealone:tcp://localhost:5210/test";
		Connection conn = DriverManager.getConnection(url, "sa", "");
		Statement stmt = conn.createStatement();

		stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long)");
		stmt.executeUpdate("INSERT INTO test(f1, f2) VALUES(1, 1)");
		stmt.executeUpdate("UPDATE test SET f2 = 2 WHERE f1 = 1");
		ResultSet rs = stmt.executeQuery("SELECT * FROM test");
		while (rs.next()) {
			System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
			System.out.println();
		}
		rs.close();
		stmt.executeUpdate("DELETE FROM test WHERE f1 = 1");
		stmt.executeUpdate("DROP TABLE IF EXISTS test");
		stmt.close();
		conn.close();
	}
}
```
把上面的代码存到一个CRUDExample.java文件 (目录随意) <br>

编译: 
> javac CRUDExample.java

运行: 
> java -cp .;E:\lealone-1.0.0\lib\lealone-client-1.0.0.jar;E:\lealone-1.0.0\lib\lealone-common-1.0.0.jar CRUDExample


# 2. 搭建Lealone集群

如果1.4小节中的Lealone还在运行，按Ctrl + C停止Lealone，<br>
接下来在单机上模拟有3个节点的Lealone集群:

* 把E:\lealone-1.0.0目录中除bin、conf、lib子目录外的东西都删除，<br>
  然后把E:\lealone-1.0.0目录拷贝3份，分别是E:\lealone-node1、E:\lealone-node2、E:\lealone-node3

* 然后分别修改各自的**conf\lealone.yaml**文件<br>
  把**run_mode**参数都改成**cluster** <br>
  把**listen_address**参数分别改成**127.0.0.1、127.0.0.2、127.0.0.3**

* 运行Lealone集群

  先运行`E:\lealone-node1\bin\lealone.bat`  <br>
  看到`Node /127.0.0.1 state jump to normal`后，Node1成功运行了；<br>
  然后运行`E:\lealone-node2\bin\lealone.bat`和`E:\lealone-node3\bin\lealone.bat` <br>
  如果在Node1的命令行控制台中看到类似下面的信息，说明Node2成功加入集群了，Node3类似: <br>
  > Node /127.0.0.2 is now part of the cluster
  
  <p>**注:** lealone.yaml文件中配置了一个seeds: **"127.0.0.1"**，<br>
  表示Node1是种子节点，所以要先运行，<br>
  Node2和Node3谁先运行不重要。

* 运行1.5小节的CRUD Example，不需要改代码，运行方式也一样
  

# 3. SQL Reference

因为Lealone的SQL引擎从[H2数据库](http://www.h2database.com/html/main.html)的SQL引擎发展而来，<br/>
所以Lealone的SQL用法与H2数据库一样。

* [SQL Grammar] (http://www.h2database.com/html/grammar.html)

* [Functions] (http://www.h2database.com/html/functions.html)

* [Data Types] (http://www.h2database.com/html/datatypes.html)

# 4. 使用Python客户端

### 4.1. 安装Python和Psycopg

* 下载[Python] (http://www.python.org/) 安装后最好将Python的根目录加入PATH环境变量
* 下载[Psycopg] (http://www.psycopg.org/psycopg/) Windows平台可以直接将Psycopg安装到Python的根目录下


### 4.2. 配置Lealone

打开**E:\lealone-1.0.0\conf\lealone.yaml**文件，<br/>
在文件最后加上下面两行代码启用pg_server: <br/>
```
pg_server_options:
    enabled: true //在enabled前面要加空格哦
```
然后重新启动Lealone，启动成功后会看到类似下面的信息:<br/>
> Lealone PgServer started, listening address: 127.0.0.1, port: 5211



### 4.3. 用Python交互式客户端访问Lealone

```
	C:\Users\Administrator> python
	
	Python 2.7.5 (default, May 15 2013, 22:43:36) [MSC v.1500 32 bit (Intel)] on win32
	Type "help", "copyright", "credits" or "license" for more information.
	
	//导入psycopg2包
	>>> import psycopg2
	

	//默认端口是5211，
	//如果是第一次执行connect，服务器端会初始化大量的与PostgreSQL相关的pg_catalog，会耗时几秒
	>>> conn = psycopg2.connect(host='localhost', port=5211, user='postgres', password='postgres', database='pg_test')
	>>> cur = conn.cursor()
	
	>>>
	>>> cur.execute("CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long)")
	>>>
	>>> cur.execute("INSERT INTO test(f1, f2) VALUES(1, 2)")
	>>>
	>>> cur.execute("SELECT * FROM test")
	>>>
	>>> cur.fetchone()
	(1, 2L)
	>>>
	>>> cur.execute("UPDATE test SET f2 = 20")
	>>>
	>>> cur.execute("SELECT * FROM test")
	>>>
	>>> cur.fetchone()
	(1, 20L)
	>>>
	>>> conn.commit()
	>>>
	>>> cur.close()
	>>>
```

# 5. 使用PostgreSQL客户端

### 5.1. 安装PostgreSQL

* [PostgreSQL Installation] (http://www.postgresql.org/docs/9.3/static/tutorial-install.html) 

* **安装PostgreSQL后，不需要启动PostgreSQL就可以通过psql访问Lealone**

### 5.2. 配置Lealone

打开**E:\lealone-1.0.0\conf\lealone.yaml**文件，<br/>
在文件最后加上下面两行代码启用pg_server: <br/>
```
pg_server_options:
    enabled: true //在enabled前面要加空格哦
```
然后重新启动Lealone，启动成功后会看到类似下面的信息:<br/>
> Lealone PgServer started, listening address: 127.0.0.1, port: 5211


### 5.3. 用psql交互式客户端访问Lealone

可以通过`psql --help`来了解psql的用法 <br>

```
C:\Users\Administrator>psql -h 127.0.0.1 -p 5211 -U test -W -d pg_test
用户 test 的口令：   //比如输入test
psql (9.3.2, 服务器 8.1.4)
输入 "help" 来获取帮助信息.

pg_test=> CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long);
UPDATE 0
pg_test=> INSERT INTO test(f1, f2) VALUES(1, 2);
INSERT 0 1
pg_test=> SELECT * FROM test;
 f1 | f2
----+----
  1 |  2
(1 rows)

pg_test=> UPDATE test SET f2 = 20;
UPDATE 1
pg_test=> SELECT * FROM test;
 f1 | f2
----+----
  1 | 20
(1 rows)

pg_test=> DELETE FROM test;
DELETE 1
pg_test=> SELECT * FROM test;
 f1 | f2
----+----
(0 rows)

pg_test=>
```