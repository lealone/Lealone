package org.lealone.test.vertx.impl;

import org.lealone.vertx.ServiceExecuter;

public class HelloWorldServiceExecuter implements ServiceExecuter {

    private final HelloWorldServiceImpl s = new HelloWorldServiceImpl();

    public HelloWorldServiceExecuter() {
    }

    @Override
    public void executeServiceNoReturnValue(String methodName, String json) {
        switch (methodName) {
        case "sayHello": {
            s.sayHello();
            break;
        }
        default: {
            System.out.println("no method: " + methodName);
        }
        }
    }

    @Override
    public String executeServiceWithReturnValue(String methodName, String json) {
        switch (methodName) {
        case "sayHello": {
            s.sayHello();
            break;
        }
        default: {
            System.out.println("no method: " + methodName);
        }
        }

        return "";
    }

}
