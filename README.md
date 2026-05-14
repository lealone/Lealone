
### Lealone 是什么

* 是一个高性能的全栈自进化通用智能体

* 能够彻底颠覆现有的应用软件开发模式

* 适用于个人助理和各种规模的企业应用


### 构建 Lealone

`mvn package -Dmaven.test.skip=true -P ec`

或者直接下载 [lealone-8.0.0-SNAPSHOT.jar](https://lealone-plugins.github.io/lealone.github.io/lealone/lealone-8.0.0-SNAPSHOT.jar)


### 启动 Lealone

`java -jar target/lealone-8.0.0-SNAPSHOT.jar`


### 打开 Lealone Agent

`java -jar target/lealone-8.0.0-SNAPSHOT.jar -agent`


### 配置大模型

在 agent 窗口中执行以下命令配置大模型，只需要执行一次:

```sql
set llm (
    provider: 'doubao', --目前只支持doubao
    model: 'doubao-seed-2-0-pro-260215',
    api_key: '替换成你的apikey'
);
```

### 个人助理

在 agent 窗口中用自然语言随意输入一段文字用分号结束


### 氛围编程

在 agent 窗口输入：实现一个todo应用，等待20多秒后会返回一个 URL，用浏览器打开即可


### 零代码零需求文档渐进式开发一个企业级 AI 应用

在 agent 窗口中执行以下命令创建第一个服务然后马上执行它:

```sql
create service if not exists my_service (
    hello(name varchar) varchar,
    get_current_time() varchar
);

execute service my_service hello('zhh');

execute service my_service get_current_time();
```


### 通过需求文档直接运行一个企业级 AI 应用

运行: `java -jar target/lealone-8.0.0-SNAPSHOT.jar services.sql`


```sql

-- 以下是 services.sql 文件的内容，也可以换成其他文件名

set llm (
    provider: 'doubao', --目前只支持doubao
    model: 'doubao-seed-2-0-pro-260215',
    api_key: '替换成你的apikey'
);

-- 下文出现的所有 url 都不是必需的，只是方便手工 copy 到浏览器测试
-- 通过以下 url 调用服务：
-- http://localhost:8080/service/my_service/hello?name=zhh
-- http://localhost:8080/service/my_service/get_current_time
create service if not exists my_service (
    hello(name varchar) varchar,
    get_current_time() varchar
);

create table if not exists user (
    id long auto_increment primary key,
    name varchar,
    age int
);

-- http://localhost:8080/service/user_service/add_user?name=zhh&age=18
-- http://localhost:8080/service/user_service/find_by_name?name=zhh
create service if not exists user_service (
    add_user(name varchar, age int) long,
    find_by_name(name varchar) user
);

-- http://localhost:8080/service/my_workflow/start?name=zhh
create workflow if not exists my_workflow (
    start(name varchar) varchar comment '找到指定的用户，然后跟他打招呼，把当前时间告诉他'
);
```


### Lealone 名字的由来

* Lealone 发音 ['li:ləʊn] 这是我新造的英文单词， <br>
  灵感来自于办公桌上那些叫绿萝的室内植物，一直想做个项目以它命名。 <br>
  绿萝的拼音是 lv luo，与 Lealone 英文发音有点相同，<br>
  Lealone 是 lea + lone 的组合，反过来念更有意思哦。:)


### [Lealone License](https://github.com/lealone/Lealone/blob/master/LICENSE.md)
