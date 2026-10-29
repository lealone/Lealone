/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.service.impl;

import java.sql.Array;

import com.lealone.db.async.Future;
import com.lealone.test.orm.generated.User;
import com.lealone.test.service.generated.UserService;

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

    @Override
    public Future<Long> addAsync(User user) {
        return user.insertAsync();
    }

    @Override
    public Future<User> findAsync(String name) {
        return User.dao.where().name.eq(name).findOneAsync();
    }

    @Override
    public Future<Integer> updateAsync(User user) {
        return user.updateAsync();
    }

    @Override
    public Future<Array> getListAsync() {
        return User.dao.findArrayAsync();
    }

    @Override
    public Future<Integer> deleteAsync(String name) {
        return User.dao.where().name.eq(name).deleteAsync();
    }
}
