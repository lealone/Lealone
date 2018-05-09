package org.lealone.test.vertx.impl;

import org.lealone.test.vertx.services.HelloWorldService;

public class HelloWorldServiceImpl implements HelloWorldService {

    @Override
    public void sayHello() {
        System.out.println("Hello World");
    }

}
