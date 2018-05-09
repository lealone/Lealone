
var sockjsUrl = "http://localhost:8080/api";

var s = lealone.getService(sockjsUrl, "hello_world_service");
s.sayHello();

s = lealone.getService(sockjsUrl, "user_service");
var user = { name: "zhh" }
s.add(user, function(result) {
    console.log(result)
});



