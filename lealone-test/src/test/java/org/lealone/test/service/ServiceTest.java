/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.service;

import java.util.UUID;

import org.junit.Test;
import org.lealone.test.orm.SqlScript;
import org.lealone.test.service.generated.AllTypeService;
import org.lealone.test.service.generated.HelloWorldService;
import org.lealone.test.sql.SqlTestBase;

public class ServiceTest extends SqlTestBase {

    @Test
    public void run() throws Exception {
        // 创建user表
        SqlScript.createUserTable(this);
        createService(this);
        callService(getURL());
    }

    private static void createService(SqlExecutor executor) {
        SqlScript.createUserService(executor);
        SqlScript.createHelloWorldService(executor);
        SqlScript.createAllTypeService(executor);
    }

    private static void callService(String url) {
        HelloWorldService helloWorldService = HelloWorldService.create(url);
        helloWorldService.sayHello();
        String r = helloWorldService.sayGoodbyeTo("zhh");
        System.out.println(r);

        System.out.println(helloWorldService.getDate());
        System.out.println(helloWorldService.getTwo("zhh", 18));

        // UserService userService = UserService.create(url);
        //
        // User user = new User().name.set("zhh").phone.set(123);
        // userService.add(user);
        //
        // user = userService.find("zhh");
        //
        // user.notes.set("call remote service");
        // userService.update(user);
        //
        // userService.delete("zhh");

        AllTypeService allTypeService = AllTypeService.create(url);
        UUID f1 = UUID.randomUUID();
        System.out.println(f1);
        f1 = allTypeService.testUuid(f1);
        System.out.println(f1);
    }
}
