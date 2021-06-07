/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.service.impl;

import java.sql.Date;

import org.lealone.test.service.generated.HelloWorldService;

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
}
