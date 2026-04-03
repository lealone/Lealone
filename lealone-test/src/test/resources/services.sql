
-- 以下参数也可以通过-D参数或通过环境变量的方式设置
set @llm_provider 'doubao'; --目前只支持doubao
set @llm_model 'doubao-seed-2-0-pro-260215';
-- set @llm_api_key '替换成你的apikey';

-- 创建 my_service 服务，默认用 java 语言实现
-- 如果逻辑简单，llm 根据名字就能知道要实现什么功能
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
