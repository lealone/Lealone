/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.lealone.engine;

import java.util.HashMap;

import org.lealone.api.ErrorCode;
import org.lealone.command.CommandInterface;
import org.lealone.command.Parser;
import org.lealone.dbobject.User;
import org.lealone.message.DbException;
import org.lealone.storage.StorageEngineManager;
import org.lealone.util.MathUtils;
import org.lealone.util.New;

/**
 * The engine contains a map of all open databases.
 * It is also responsible for opening and creating new databases.
 * This is a singleton class.
 */
public class DatabaseEngine implements SessionFactory {
    private static final HashMap<String, Database> DATABASES = New.hashMap();
    private static final DatabaseEngine INSTANCE = new DatabaseEngine();

    private static String hostAndPort;

    public static String getHostAndPort() {
        return hostAndPort;
    }

    public static DatabaseEngine getInstance() {
        return INSTANCE;
    }

    public static synchronized void init(String host, int port) {
        hostAndPort = host + ":" + port;

        StorageEngineManager.initStorageEngines();
        SystemDatabase.init();

        for (String dbName : SystemDatabase.findAll())
            DATABASES.put(dbName, new Database(INSTANCE, true));
    }

    private volatile long wrongPasswordDelay = SysProperties.DELAY_WRONG_PASSWORD_MIN;

    protected DatabaseEngine() {
    }

    public Database createDatabase(boolean persistent) {
        return new Database(this, persistent);
    }

    /**
     * Called after a database has been closed, to remove the object from the
     * list of open databases.
     *
     * @param dbName the database name
     */
    public synchronized void closeDatabase(String dbName) {
        DATABASES.remove(dbName);
    }

    @Override
    public synchronized Session createSession(ConnectionInfo ci) {
        return createSessionAndValidate(ci);
    }

    private Session createSessionAndValidate(ConnectionInfo ci) {
        try {
            boolean ifExists = ci.getProperty("IFEXISTS", false);
            Session session;
            for (int i = 0;; i++) {
                session = createSession(ci, ifExists);
                if (session != null) {
                    break;
                }
                // we found a database that is currently closing
                // wait a bit to avoid a busy loop (the method is synchronized)
                if (i > 60 * 1000) {
                    // retry at most 1 minute
                    throw DbException.get(ErrorCode.DATABASE_ALREADY_OPEN_1,
                            "Waited for database closing longer than 1 minute");
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    // ignore
                }
            }

            initSession(session, ci);
            validateUserAndPassword(true);
            return session;
        } catch (DbException e) {
            if (e.getErrorCode() == ErrorCode.WRONG_USER_OR_PASSWORD) {
                validateUserAndPassword(false);
            }
            throw e;
        }
    }

    private Session createSession(ConnectionInfo ci, boolean ifExists) {
        String name = ci.getDatabaseName();
        name = Database.parseDatabaseShortName(ci.getDbSettings(), name);
        Database database;
        boolean openNew = ci.getProperty("OPEN_NEW", false);
        if (openNew) {
            database = null;
        } else {
            database = DATABASES.get(name);
        }
        User user = null;
        boolean opened = false;
        if (database == null) {
            if (ifExists && !SystemDatabase.exists(name)) {
                throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_1, name);
            }
            database = createDatabase(ci.isPersistent());
            database.init(ci, name);
            opened = true;
            if (database.getAllUsers().isEmpty()) {
                // users is the last thing we add, so if no user is around,
                // the database is new (or not initialized correctly)
                user = new User(database, database.allocateObjectId(), ci.getUserName(), false);
                user.setAdmin(true);
                user.setUserPasswordHash(ci.getUserPasswordHash());
                database.setMasterUser(user);
            }
            DATABASES.put(name, database);

            if (database.isPersistent())
                SystemDatabase.addDatabase(database.getShortName(), database.getStorageEngineName());
        } else {
            if (!database.isInitialized())
                database.init(ci, name);
        }

        synchronized (database) {
            if (opened) {
                // start the thread when already synchronizing on the database
                // otherwise a deadlock can occur when the writer thread
                // opens a new database (as in recovery testing)
                database.opened();
            }
            if (database.isClosing()) {
                return null;
            }
            if (user == null) {
                if (database.validateFilePasswordHash(ci.getProperty("CIPHER", null), ci.getFilePasswordHash())) {
                    user = database.findUser(ci.getUserName());
                    if (user != null) {
                        if (!user.validateUserPasswordHash(ci.getUserPasswordHash())) {
                            user = null;
                        }
                    }
                }
                if (opened && (user == null || !user.isAdmin())) {
                    // reset - because the user is not an admin, and has no
                    // right to listen to exceptions
                    database.setEventListener(null);
                }
            }
            if (user == null) {
                database.removeSession(null);
                throw DbException.get(ErrorCode.WRONG_USER_OR_PASSWORD);
            }
            Session session = database.createSession(user);
            session.setConnectionInfo(ci);
            return session;
        }
    }

    private void initSession(Session session, ConnectionInfo ci) {
        boolean ignoreUnknownSetting = ci.getProperty("IGNORE_UNKNOWN_SETTINGS", false);
        String init = ci.getProperty("INIT", null);

        session.setAllowLiterals(true);
        CommandInterface command;
        for (String setting : ci.getKeys()) {
            if (SetTypes.contains(setting)) {
                String value = ci.getProperty(setting);
                try {
                    command = session.prepareCommandLocal("SET " + Parser.quoteIdentifier(setting) + " " + value);
                    command.executeUpdate();
                } catch (DbException e) {
                    if (!ignoreUnknownSetting) {
                        session.close();
                        throw e;
                    }
                }
            }
        }
        if (init != null) {
            try {
                command = session.prepareCommand(init, Integer.MAX_VALUE);
                command.executeUpdate();
            } catch (DbException e) {
                if (!ignoreUnknownSetting) {
                    session.close();
                    throw e;
                }
            }
        }
        session.setAllowLiterals(false);
        session.commit(true);
    }

    /**
     * This method is called after validating user name and password. If user
     * name and password were correct, the sleep time is reset, otherwise this
     * method waits some time (to make brute force / rainbow table attacks
     * harder) and then throws a 'wrong user or password' exception. The delay
     * is a bit randomized to protect against timing attacks. Also the delay
     * doubles after each unsuccessful logins, to make brute force attacks
     * harder.
     *
     * There is only one exception message both for wrong user and for
     * wrong password, to make it harder to get the list of user names. This
     * method must only be called from one place, so it is not possible from the
     * stack trace to see if the user name was wrong or the password.
     *
     * @param correct if the user name or the password was correct
     * @throws DbException the exception 'wrong user or password'
     */
    private void validateUserAndPassword(boolean correct) {
        int min = SysProperties.DELAY_WRONG_PASSWORD_MIN;
        if (correct) {
            long delay = wrongPasswordDelay;
            if (delay > min && delay > 0) {
                // the first correct password must be blocked,
                // otherwise parallel attacks are possible
                synchronized (this) {
                    // delay up to the last delay
                    // an attacker can't know how long it will be
                    delay = MathUtils.secureRandomInt((int) delay);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    wrongPasswordDelay = min;
                }
            }
        } else {
            // this method is not synchronized on the Engine, so that
            // regular successful attempts are not blocked
            synchronized (this) {
                long delay = wrongPasswordDelay;
                int max = SysProperties.DELAY_WRONG_PASSWORD_MAX;
                if (max <= 0) {
                    max = Integer.MAX_VALUE;
                }
                wrongPasswordDelay += wrongPasswordDelay;
                if (wrongPasswordDelay > max || wrongPasswordDelay < 0) {
                    wrongPasswordDelay = max;
                }
                if (min > 0) {
                    // a bit more to protect against timing attacks
                    delay += Math.abs(MathUtils.secureRandomLong() % 100);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                throw DbException.get(ErrorCode.WRONG_USER_OR_PASSWORD);
            }
        }
    }
}
