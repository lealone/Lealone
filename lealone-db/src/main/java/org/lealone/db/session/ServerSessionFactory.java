/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.session;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.ConnectionSetting;
import org.lealone.db.Database;
import org.lealone.db.DbSetting;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.Future;
import org.lealone.db.auth.User;

/**
 * This class is responsible for creating new sessions.
 * This is a singleton class.
 * 
 * @author H2 Group
 * @author zhh
 */
public class ServerSessionFactory implements SessionFactory {

    private static final ServerSessionFactory instance = new ServerSessionFactory();

    public static ServerSessionFactory getInstance() {
        return instance;
    }

    private ServerSessionFactory() {
    }

    @Override
    public Future<Session> createSession(ConnectionInfo ci) {
        return Future.succeededFuture(createServerSession(ci));
    }

    private ServerSession createServerSession(ConnectionInfo ci) {
        String dbName = ci.getDatabaseName();
        // 内嵌数据库，如果不存在，则自动创建
        if (ci.isEmbedded() && LealoneDatabase.getInstance().findDatabase(dbName) == null) {
            LealoneDatabase.getInstance().createEmbeddedDatabase(dbName, ci);
        }
        ServerSession session = createServerSession(dbName, ci);
        initSession(session, ci);
        return session;
    }

    private ServerSession createServerSession(String dbName, ConnectionInfo ci) {
        Database database = LealoneDatabase.getInstance().getDatabase(dbName);

        // 如果数据库正在关闭过程中，不等待重试了，直接抛异常
        // 如果数据库已经关闭了，那么在接下来的init中会重新打开
        if (database.isClosing()) {
            throw DbException.get(ErrorCode.DATABASE_IS_CLOSING);
        }
        if (!database.isInitialized()) {
            database.init();
        }

        User user = null;
        if (database.validateFilePasswordHash(ci.getProperty(DbSetting.CIPHER.getName(), null),
                ci.getFilePasswordHash())) {
            user = database.findUser(null, ci.getUserName());
            if (user != null) {
                if (!user.validateUserPasswordHash(ci.getUserPasswordHash())) {
                    user = null;
                }
            }
        }
        if (user == null) {
            database.removeSession(null);
            throw DbException.get(ErrorCode.WRONG_USER_OR_PASSWORD);
        }
        ServerSession session = database.createSession(user, ci);
        session.setRunMode(database.getRunMode());
        return session;
    }

    private void initSession(ServerSession session, ConnectionInfo ci) {
        boolean autoCommit = session.isAutoCommit();
        session.setAutoCommit(false);
        session.setRoot(ci.getProperty(ConnectionSetting.IS_ROOT, true));
        boolean ignoreUnknownSetting = ci.getProperty(ConnectionSetting.IGNORE_UNKNOWN_SETTINGS, false);
        session.setAllowLiterals(true);
        for (String setting : ci.getKeys()) {
            if (SessionSetting.contains(setting) || DbSetting.contains(setting)) {
                String value = ci.getProperty(setting);
                try {
                    String sql = "SET " + session.getDatabase().quoteIdentifier(setting) + " '" + value + "'";
                    session.prepareStatementLocal(sql).executeUpdate();
                } catch (DbException e) {
                    if (!ignoreUnknownSetting) {
                        session.close();
                        throw e;
                    }
                }
            }
        }
        String init = ci.getProperty(ConnectionSetting.INIT, null);
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
        session.commit();
        session.setAutoCommit(autoCommit);
    }
}
