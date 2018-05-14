/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.test.service;

import org.lealone.test.generated.model.User;
import org.lealone.test.generated.service.HelloWorldService;
import org.lealone.test.generated.service.UserService;

public class ServiceConsumer {

    public static void callService(String url) {
        HelloWorldService helloWorldService = HelloWorldService.create(url);
        helloWorldService.sayHello();
        String r = helloWorldService.sayGoodbyeTo("zhh");
        System.out.println(r);

        UserService userService = UserService.create(url);

        User user = new User().name.set("zhh").phone.set(123);
        userService.add(user);

        user = userService.find("zhh");

        user.notes.set("call remote service");
        userService.update(user);

        userService.delete("zhh");
    }

}
