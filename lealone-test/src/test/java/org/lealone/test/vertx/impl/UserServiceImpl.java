package org.lealone.test.vertx.impl;

import org.lealone.test.vertx.services.User;
import org.lealone.test.vertx.services.UserService;

public class UserServiceImpl implements UserService {

    @Override
    public User add(User user) {
        return user.setId(1000L);
    }

    @Override
    public User find(long id) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean update(User user) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean delete(long id) {
        // TODO Auto-generated method stub
        return false;
    }

}
