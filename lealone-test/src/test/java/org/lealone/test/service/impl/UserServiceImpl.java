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
package org.lealone.test.service.impl;

import java.sql.Date;

import org.lealone.test.service.generated.User;
import org.lealone.test.service.generated.UserService;

public class UserServiceImpl implements UserService {

    @Override
    public User add(User user) {
        return user.setId(1000L);
    }

    @Override
    public User find(Long id) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public User findByDate(Date d) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean update(User user) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean delete(Long id) {
        // TODO Auto-generated method stub
        return null;
    }

}
