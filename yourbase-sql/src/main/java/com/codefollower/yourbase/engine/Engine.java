/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.yourbase.engine;

import java.util.HashMap;

import com.codefollower.yourbase.command.CommandInterface;
import com.codefollower.yourbase.command.Parser;
import com.codefollower.yourbase.constant.DbSettings;
import com.codefollower.yourbase.constant.ErrorCode;
import com.codefollower.yourbase.constant.SetTypes;
import com.codefollower.yourbase.constant.SysProperties;
import com.codefollower.yourbase.dbobject.User;
import com.codefollower.yourbase.engine.ConnectionInfo;
import com.codefollower.yourbase.engine.Constants;
import com.codefollower.yourbase.engine.SessionFactory;
import com.codefollower.yourbase.jmx.DatabaseInfo;
import com.codefollower.yourbase.message.DbException;
import com.codefollower.yourbase.util.MathUtils;
import com.codefollower.yourbase.util.New;
import com.codefollower.yourbase.util.StringUtils;
import com.codefollower.yourbase.util.Utils;

/**
 * The engine contains a map of all open databases.
 * It is also responsible for opening and creating new databases.
 * This is a singleton class.
 */
public class Engine implements SessionFactory {

    private static final Engine INSTANCE = new Engine();
    private static final HashMap<String, Database> DATABASES = New.hashMap();

    private volatile long wrongPasswordDelay = SysProperties.DELAY_WRONG_PASSWORD_MIN;
    private boolean jmx;

    public static Engine getInstance() {
        return INSTANCE;
    }

    protected Database createDatabase() {
        return new Database();
    }

    private Session openSession(ConnectionInfo ci, boolean ifExists, String cipher) {
        String name = ci.getName();
        Database database;
        ci.removeProperty("NO_UPGRADE", false);
        boolean openNew = ci.getProperty("OPEN_NEW", false);
        if (openNew || ci.isUnnamedInMemory()) {
            database = null;
        } else {
            database = DATABASES.get(name);
        }
        User user = null;
        boolean opened = false;
        if (database == null) {
            if (ifExists && !Database.exists(name)) {
                throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_1, name);
            }
            database = createDatabase();
            database.init(ci, cipher);
            opened = true;
            if (database.getAllUsers().size() == 0) {
                // users is the last thing we add, so if no user is around,
                // the database is new (or not initialized correctly)
                user = new User(database, database.allocateObjectId(), ci.getUserName(), false);
                user.setAdmin(true);
                user.setUserPasswordHash(ci.getUserPasswordHash());
                database.setMasterUser(user);
            }
            if (!ci.isUnnamedInMemory()) {
                DATABASES.put(name, database);
            }
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
                if (database.validateFilePasswordHash(cipher, ci.getFilePasswordHash())) {
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
            checkClustering(ci, database);
            Session session = database.createSession(user);
            if (ci.getProperty("JMX", false)) {
                try {
                    Utils.callStaticMethod(DatabaseInfo.class.getName() + ".registerMBean", ci, database);
                } catch (Exception e) {
                    database.removeSession(session);
                    throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1, e, "JMX");
                }
                jmx = true;
            }
            return session;
        }
    }

    /**
     * Open a database connection with the given connection information.
     *
     * @param ci the connection information
     * @return the session
     */
    public Session createSession(ConnectionInfo ci) {
        return INSTANCE.createSessionAndValidate(ci);
    }

    protected Session createSessionAndValidate(ConnectionInfo ci) {
        try {

            Session session = openSession(ci);
            validateUserAndPassword(true);
            return session;
        } catch (DbException e) {
            if (e.getErrorCode() == ErrorCode.WRONG_USER_OR_PASSWORD) {
                validateUserAndPassword(false);
            }
            throw e;
        }
    }

    protected synchronized Session openSession(ConnectionInfo ci) {
        boolean ifExists = ci.removeProperty("IFEXISTS", false);
        boolean ignoreUnknownSetting = ci.removeProperty("IGNORE_UNKNOWN_SETTINGS", false);
        String cipher = ci.removeProperty("CIPHER", null);
        String init = ci.removeProperty("INIT", null);
        Session session;
        while (true) {
            session = openSession(ci, ifExists, cipher);
            if (session != null) {
                break;
            }
            // we found a database that is currently closing
            // wait a bit to avoid a busy loop (the method is synchronized)
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                // ignore
            }
        }

		ci.removeProperty("SERVER_TYPE", false);
        session.setAllowLiterals(true);
        DbSettings defaultSettings = DbSettings.getInstance(null);
        for (String setting : ci.getKeys()) {
            if (defaultSettings.containsKey(setting)) {
                // database setting are only used when opening the database
                continue;
            }
            String value = ci.getProperty(setting);
            try {
                CommandInterface command = session.prepareLocal("SET " + Parser.quoteIdentifier(setting) + " "
                        + value);
                command.executeUpdate();
            } catch (DbException e) {
                if (!ignoreUnknownSetting) {
                    session.close();
                    throw e;
                }
            }
        }
        if (init != null) {
            try {
                CommandInterface command = session.prepareCommand(init, Integer.MAX_VALUE);
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
        return session;
    }

    private static void checkClustering(ConnectionInfo ci, Database database) {
        String clusterSession = ci.getProperty(SetTypes.CLUSTER, null);
        if (Constants.CLUSTERING_DISABLED.equals(clusterSession)) {
            // in this case, no checking is made
            // (so that a connection can be made to disable/change clustering)
            return;
        }
        String clusterDb = database.getCluster();
        if (!Constants.CLUSTERING_DISABLED.equals(clusterDb)) {
            if (!Constants.CLUSTERING_ENABLED.equals(clusterSession)) {
                if (!StringUtils.equals(clusterSession, clusterDb)) {
                    if (clusterDb.equals(Constants.CLUSTERING_DISABLED)) {
                        throw DbException.get(ErrorCode.CLUSTER_ERROR_DATABASE_RUNS_ALONE);
                    }
                    throw DbException.get(ErrorCode.CLUSTER_ERROR_DATABASE_RUNS_CLUSTERED_1, clusterDb);
                }
            }
        }
    }

    /**
     * Called after a database has been closed, to remove the object from the
     * list of open databases.
     *
     * @param name the database name
     */
    void close(String name) {
        if (jmx) {
            try {
                Utils.callStaticMethod(DatabaseInfo.class.getName() + ".unregisterMBean", name);
            } catch (Exception e) {
                throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1, e, "JMX");
            }
        }
        DATABASES.remove(name);
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
    protected void validateUserAndPassword(boolean correct) {
        int min = SysProperties.DELAY_WRONG_PASSWORD_MIN;
        if (correct) {
            long delay = wrongPasswordDelay;
            if (delay > min && delay > 0) {
                // the first correct password must be blocked,
                // otherwise parallel attacks are possible
                synchronized (INSTANCE) {
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
            synchronized (INSTANCE) {
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
