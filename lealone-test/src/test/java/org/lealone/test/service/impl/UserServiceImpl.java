/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.service.impl;

import java.sql.Array;

import org.lealone.test.orm.generated.User;
import org.lealone.test.service.generated.UserService;

public class UserServiceImpl implements UserService {

    @Override
    public Long add(User user) {
        return user.insert();
    }

    @Override
    public User find(String name) {
        return User.dao.where().name.eq(name).findOne();
    }

    @Override
    public Integer update(User user) {
        return user.update();
    }

    @Override
    public Integer delete(String name) {
        return User.dao.where().name.eq(name).delete();
    }

    @Override
    public Array getList() {
        return User.dao.findArray();
    }

}