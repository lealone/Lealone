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
package org.lealone.db.auth;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.api.ErrorCode;
import org.lealone.common.message.DbException;
import org.lealone.common.util.New;
import org.lealone.db.Constants;
import org.lealone.db.LealoneDatabase;

/**
 * 
 * 管理所有User、Role、Right
 * 
 * @author zhh
 */
public class Auth {

    /**
     * The default name of the system user. This name is only used as long as
     * there is no administrator user registered.
     */
    private static final String SYSTEM_USER_NAME = "DBA";

    private static final ConcurrentHashMap<String, Role> roles = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, User> users = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Right> rights = new ConcurrentHashMap<>();

    private static User systemUser;
    private static Role publicRole;

    public static synchronized void init(LealoneDatabase db) {
        if (systemUser == null) {
            systemUser = new User(db, 0, SYSTEM_USER_NAME, true);
            systemUser.setAdmin(true);

            publicRole = new Role(db, 0, Constants.PUBLIC_ROLE_NAME, true);
            roles.put(Constants.PUBLIC_ROLE_NAME, publicRole);
        }
    }

    private Auth() {
    }

    public static User getSystemUser() {
        return systemUser;
    }

    public static Role getPublicRole() {
        return publicRole;
    }

    /**
     * Get the role if it exists, or null if not.
     *
     * @param roleName the name of the role
     * @return the role or null
     */
    public static Role findRole(String roleName) {
        return roles.get(roleName);
    }

    public static ArrayList<Role> getAllRoles() {
        return New.arrayList(roles.values());
    }

    public static Map<String, Role> getRolesMap() {
        return roles;
    }

    /**
     * Get user with the given name. This method throws an exception if the user
     * does not exist.
     *
     * @param name the user name
     * @return the user
     * @throws DbException if the user does not exist
     */
    public static User getUser(String name) {
        User user = findUser(name);
        if (user == null) {
            throw DbException.get(ErrorCode.USER_NOT_FOUND_1, name);
        }
        return user;
    }

    /**
     * Get the user if it exists, or null if not.
     *
     * @param name the name of the user
     * @return the user or null
     */
    public static User findUser(String name) {
        return users.get(name);
    }

    public static ArrayList<User> getAllUsers() {
        return New.arrayList(users.values());
    }

    public static Map<String, User> getUsersMap() {
        return users;
    }

    public static ArrayList<Right> getAllRights() {
        return New.arrayList(rights.values());
    }

    public static Map<String, Right> getRightsMap() {
        return rights;
    }
}
