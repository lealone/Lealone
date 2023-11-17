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
import org.lealone.db.Mode;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.Future;
import org.lealone.db.auth.User;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.scheduler.SchedulerThread;
import org.lealone.net.NetNode;
import org.lealone.transaction.TransactionListener;

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
    public Future<Session> createSession(ConnectionInfo ci, boolean allowRedirect) {
        return Future.succeededFuture(createServerSession(ci));
    }

    private ServerSession createServerSession(ConnectionInfo ci) {
        String dbName = ci.getDatabaseName();
        // 内嵌数据库，如果不存在，则自动创建
        if (ci.isEmbedded() && LealoneDatabase.getInstance().findDatabase(dbName) == null) {
            LealoneDatabase.getInstance().createEmbeddedDatabase(dbName, ci);
        }
        ServerSession session = createServerSession(dbName, ci);
        if (session == null) {
            return null;
        }
        if (session.isInvalid()) { // 无效session，不需要进行后续的操作
            return session;
        }
        initSession(session, ci);
        return session;
    }

    private ServerSession createServerSession(String dbName, ConnectionInfo ci) {
        Database database = LealoneDatabase.getInstance().getDatabase(dbName);
        String targetNodes;
        if (ci.isEmbedded()) {
            targetNodes = null;
        } else {
            NetNode localNode = NetNode.getLocalTcpNode();
            targetNodes = database.getTargetNodes();
            // 为null时总是认为当前节点就是数据库所在的节点
            if (targetNodes == null) {
                targetNodes = localNode.getHostAndPort();
            } else if (!database.isTargetNode(localNode)) {
                ServerSession session = new ServerSession(database,
                        LealoneDatabase.getInstance().getSystemSession().getUser(), 0);
                session.setTargetNodes(targetNodes);
                session.setRunMode(database.getRunMode());
                session.setInvalid(true);
                return session;
            }
        }

        // 如果数据库正在关闭过程中，不等待重试了，直接抛异常
        // 如果数据库已经关闭了，那么在接下来的init中会重新打开
        if (database.isClosing()) {
            throw DbException.get(ErrorCode.DATABASE_IS_CLOSING);
        }
        if (!database.isInitialized() && !initDatabase(database, ci)) {
            return null;
        }
        User user = validateUser(database, ci);
        ServerSession session = database.createSession(user, ci);
        session.setTargetNodes(targetNodes);
        session.setRunMode(database.getRunMode());
        return session;
    }

    // 只能有一个线程初始化数据库
    private boolean initDatabase(Database database, ConnectionInfo ci) {
        ServerSession session = new ServerSession(database, null, 0);
        TransactionListener tl = SchedulerThread.currentTransactionListener();
        session.setTransactionListener(tl);
        DbObjectLock lock = database.tryExclusiveDatabaseLock(session);
        if (lock != null) {
            try {
                // sharding模式下访问remote page时会用到
                database.setLastConnectionInfo(ci);
                database.init();
            } finally {
                session.commit();
            }
            return true;
        } else {
            // 仅仅是从事务引擎中删除事务
            session.getTransaction().rollback();
            return false;
        }
    }

    private User validateUser(Database database, ConnectionInfo ci) {
        User user = null;
        if (database.validateFilePasswordHash(ci.getProperty(DbSetting.CIPHER.getName(), null),
                ci.getFilePasswordHash())) {
            user = database.findUser(null, ci.getUserName());
            if (user != null) {
                Mode mode = Mode.getInstance(
                        ci.getProperty(DbSetting.MODE.name(), Mode.getDefaultMode().getName()));
                if (!user.validateUserPasswordHash(ci.getUserPasswordHash(), ci.getSalt(), mode)) {
                    user = null;
                } else {
                    database.setLastConnectionInfo(ci);
                }
            }
        }
        if (user == null) {
            database.removeSession(null);
            throw DbException.get(ErrorCode.WRONG_USER_OR_PASSWORD);
        }
        return user;
    }

    private void initSession(ServerSession session, ConnectionInfo ci) {
        String[] keys = ci.getKeys();
        if (keys.length == 0)
            return;
        boolean autoCommit = session.isAutoCommit();
        session.setAutoCommit(false);
        session.setAllowLiterals(true);
        boolean ignoreUnknownSetting = ci.getProperty(ConnectionSetting.IGNORE_UNKNOWN_SETTINGS, false);
        for (String key : ci.getKeys()) {
            if (SessionSetting.contains(key) || DbSetting.contains(key)) {
                try {
                    String sql = "SET " + session.getDatabase().quoteIdentifier(key) + " '"
                            + ci.getProperty(key) + "'";
                    session.prepareStatementLocal(sql).executeUpdate();
                } catch (DbException e) {
                    if (!ignoreUnknownSetting) {
                        session.close();
                        throw e;
                    }
                }
            }
        }
        session.commit();
        session.setAutoCommit(autoCommit);
        session.setAllowLiterals(false);
    }
}
