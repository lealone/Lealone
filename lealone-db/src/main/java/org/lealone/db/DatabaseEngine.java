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
package org.lealone.db;

import java.util.List;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.MathUtils;
import org.lealone.db.auth.User;

/**
 * The engine is responsible for creating new sessions.
 * This is a singleton class.
 * 
 * @author H2 Group
 * @author zhh
 */
public class DatabaseEngine {

    private static final SessionFactoryImpl SESSION_FACTORY = new SessionFactoryImpl();

    public static List<Database> getDatabases() {
        return LealoneDatabase.getInstance().getDatabases();
    }

    public static SessionFactory getSessionFactory() {
        return SESSION_FACTORY;
    }

    public static ServerSession createSession(String url) {
        return SESSION_FACTORY.createSession(new ConnectionInfo(url));
    }

    public static ServerSession createSession(ConnectionInfo ci) {
        return SESSION_FACTORY.createSession(ci);
    }

    private DatabaseEngine() {
    }

    private static class SessionFactoryImpl implements SessionFactory {

        private volatile long wrongPasswordDelay = SysProperties.DELAY_WRONG_PASSWORD_MIN;

        @Override
        public synchronized ServerSession createSession(ConnectionInfo ci) {
            String dbName = ci.getDatabaseName();
            dbName = Database.parseDatabaseShortName(ci.getDbSettings(), dbName);

            try {
                boolean ifExists = ci.getProperty("IFEXISTS", false);
                ServerSession session;
                for (int i = 0;; i++) {
                    session = createSession(dbName, ci, ifExists);
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

        private ServerSession createSession(String dbName, ConnectionInfo ci, boolean ifExists) {
            // 不允许Client访问LealoneDatabase
            if (ci.isRemote() && LealoneDatabase.NAME.equalsIgnoreCase(dbName))
                throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_1, dbName);

            boolean opened = false;
            User user = null;
            Database database = LealoneDatabase.getInstance().findDatabase(dbName);
            if (database == null) {
                if (ifExists)
                    throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_1, dbName);
                database = LealoneDatabase.getInstance().createDatabase(dbName, ci);
                database.init(ci);
                opened = true;
                if (database.getAllUsers().isEmpty()) {
                    // users is the last thing we add, so if no user is around,
                    // the database is new (or not initialized correctly)
                    user = new User(database, database.allocateObjectId(), ci.getUserName(), false);
                    user.setAdmin(true);
                    user.setUserPasswordHash(ci.getUserPasswordHash());
                    database.setMasterUser(user);
                }
            } else {
                if (!database.isInitialized())
                    database.init(ci);
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
                ServerSession session = database.createSession(user);
                session.setConnectionInfo(ci);
                return session;
            }
        }

        private void initSession(ServerSession session, ConnectionInfo ci) {
            boolean ignoreUnknownSetting = ci.getProperty("IGNORE_UNKNOWN_SETTINGS", false);
            String init = ci.getProperty("INIT", null);

            session.setAllowLiterals(true);
            for (String setting : ci.getKeys()) {
                if (SetTypes.contains(setting)) {
                    String value = ci.getProperty(setting);
                    try {
                        String sql = "SET " + session.getDatabase().quoteIdentifier(setting) + " " + value;
                        session.prepareStatementLocal(sql).executeUpdate();
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
                    session.prepareStatement(init, Integer.MAX_VALUE).executeUpdate();
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
}
