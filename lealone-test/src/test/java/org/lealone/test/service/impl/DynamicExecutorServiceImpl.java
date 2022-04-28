/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.service.impl;

import org.lealone.test.orm.generated.User;

// 动态创建ServiceExecutor
public class DynamicExecutorServiceImpl {

    public Long add(User user) {
        return user.insert();
    }

    public Integer delete(String name) {
        return User.dao.where().name.eq(name).delete();
    }

    public User find(String name) {
        return User.dao.where().name.eq(name).findOne();
    }
}
