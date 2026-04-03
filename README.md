
### Lealone 是什么

* 是一个安全的能够自我进化的 AI 应用开发平台

* 能够彻底颠覆现有的应用软件开发模式


### 快速入门

构建: `mvn package -Dmaven.test.skip=true -P ai`

运行: `java -jar target/lealone-8.0.0-SNAPSHOT.jar services.sql`


```sql

-- 以下是 services.sql 文件的内容，也可用换成其他文件名

set @llm_provider 'doubao'; --目前只支持doubao
set @llm_model 'doubao-seed-2-0-pro-260215';
set @llm_api_key '替换成你的apikey';

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
