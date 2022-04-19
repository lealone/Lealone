/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.service;

import org.junit.Test;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.orm.SqlScript;
import org.lealone.test.service.generated.HelloWorldService;
import org.lealone.test.sql.DSqlTestBase;

public class ExecuteDServiceTest extends DSqlTestBase {

    public ExecuteDServiceTest() {
        super(LealoneDatabase.NAME);
    }

    @Test
    public void run() throws Exception {
        String dbName = ExecuteDServiceTest.class.getSimpleName();
        executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE replication "
                + "PARAMETERS (replication_strategy: 'SimpleStrategy', replication_factor: 2)");

        class ServiceExecutorTest extends DSqlTestBase {
            public ServiceExecutorTest(String dbName) {
                super(dbName);
                setHost("127.0.0.3");
            }

            @Override
            protected void test() throws Exception {
                SqlScript.createHelloWorldService(this);
                HelloWorldService helloWorldService = HelloWorldService.create(getURL());
                helloWorldService.sayHello();
                String r = helloWorldService.sayGoodbyeTo("zhh");
                System.out.println(r);
            }
        }
        new ServiceExecutorTest(dbName).runTest();
    }
}
