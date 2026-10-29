/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.service.impl;

import java.sql.Date;

import com.lealone.db.async.Future;
import com.lealone.test.service.generated.HelloWorldService;

public class HelloWorldServiceImpl implements HelloWorldService {

    @Override
    public void sayHello() {
        System.out.println("Hello World");
    }

    @Override
    public String sayGoodbyeTo(String name) {
        return "Bye " + name;
    }

    @Override
    public Date getDate() {
        return new Date(System.currentTimeMillis());
    }

    @Override
    public Integer getInt() {
        return 0;
    }

    @Override
    public Integer getTwo(String name, Integer age) {
        System.out.println("getTwo, name: " + name);
        return age + 10;
    }

    @Override
    public Future<Void> sayHelloAsync() {
        sayHello();
        return Future.succeededFuture(null);
    }

    @Override
    public Future<Date> getDateAsync() {
        return Future.succeededFuture(getDate());
    }

    @Override
    public Future<Integer> getIntAsync() {
        return Future.succeededFuture(getInt());
    }

    @Override
    public Future<Integer> getTwoAsync(String name, Integer age) {
        return Future.succeededFuture(getTwo(name, age));
    }

    @Override
    public Future<String> sayGoodbyeToAsync(String name) {
        return Future.succeededFuture(sayGoodbyeTo(name));
    }
}
