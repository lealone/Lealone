
set @llm_provider 'doubao'; --目前只支持doubao
set @llm_model 'doubao-seed-2-0-pro-260215';
-- 也可以通过-Dllm_api_key=xxx的方式传递
-- set @llm_api_key '替换成你的apikey';

-- 创建 my_service 服务，默认用 java 语言实现
-- 如果逻辑简单，llm 根据名字就能知道要实现什么功能
-- 通过以下 url 调用服务：
-- http://localhost:8080/service/my_service/hello?name=zhh
-- http://localhost:8080/service/my_service/get_current_time
create service if not exists my_service (
    hello(name varchar) varchar,
    get_current_time() varchar
)
implement by 'com.lealone.examples.MyService'; --这是可选的，加上之后可以指定生成的类名

