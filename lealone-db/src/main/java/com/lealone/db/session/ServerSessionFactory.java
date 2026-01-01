/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.session;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.ConnectionInfo;
import com.lealone.db.Database;
import com.lealone.db.DbSetting;
import com.lealone.db.LealoneDatabase;
import com.lealone.db.Mode;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.async.AsyncCallback;
import com.lealone.db.async.Future;
import com.lealone.db.auth.User;
import com.lealone.db.scheduler.EmbeddedScheduler;
import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.scheduler.SchedulerFactory;
import com.lealone.db.scheduler.SchedulerLock;
import com.lealone.db.scheduler.SchedulerThread;

public class ServerSessionFactory extends SessionFactoryBase {

    private static final ServerSessionFactory instance = new ServerSessionFactory();

    public static ServerSessionFactory getInstance() {
        return instance;
    }

    @Override
    public Future<Session> createSession(ConnectionInfo ci, boolean allowRedirect) {
        if (SchedulerThread.isScheduler()) {
            return Future.succeededFuture(createServerSession(ci));
        } else {
            // 在嵌入模式下自动启动EmbeddedScheduler，然后在EmbeddedScheduler中运行createSession
            if (ci.isEmbedded()) {
                SchedulerFactory sf = SchedulerFactory.getDefaultSchedulerFactory();
                if (sf == null)
                    sf = SchedulerFactory.getSchedulerFactory(EmbeddedScheduler.class, ci.getConfig());
                Scheduler scheduler = sf.getScheduler();
                ci.setScheduler(scheduler);
                AsyncCallback<Session> ac = AsyncCallback.createConcurrentCallback();
                scheduler.handle(() -> {
                    try {
                        ac.setAsyncResult(createServerSession(ci));
                    } catch (Exception e) {
                        ac.setAsyncResult(e);
                    }
                });
                return ac;
            } else {
                throw DbException.getInternalError();
            }
        }
    }

    private ServerSession createServerSession(ConnectionInfo ci) {
        LealoneDatabase ldb = LealoneDatabase.getInstance();
        String dbName = ci.getDatabaseName();
        // 内嵌数据库，如果不存在，则自动创建
        if (ci.isEmbedded() && ldb.findDatabase(dbName) == null) {
            ldb.createEmbeddedDatabase(dbName, ci);
        }
        Database database = ldb.getDatabase(dbName);
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
        session.setRunMode(database.getRunMode());
        return session;
    }

    // 只能有一个线程初始化数据库
    private boolean initDatabase(Database database, ConnectionInfo ci) {
        SchedulerLock schedulerLock = database.getSchedulerLock();
        if (schedulerLock.tryLock((InternalScheduler) SchedulerThread.currentScheduler())) {
            try {
                // sharding模式下访问remote page时会用到
                database.setLastConnectionInfo(ci);
                database.init();
            } finally {
                schedulerLock.unlock();
            }
            return true;
        }
        return false;
    }

    private User validateUser(Database database, ConnectionInfo ci) {
        User user = null;
        if (database.validateFilePasswordHash(ci.getProperty(DbSetting.CIPHER.getName(), null),
                ci.getFilePasswordHash())) {
            user = database.findUser(null, ci.getUserName());
            if (user != null) {
                // 如果是嵌入模式且是安全模式就不需要验证密码，此时可以重置密码
                if (!(ci.isEmbedded() && ci.isSafeMode())) {
                    Mode mode = Mode.getInstance(
                            ci.getProperty(DbSetting.MODE.name(), Mode.getDefaultMode().getName()));
                    if (!user.validateUserPasswordHash(ci.getUserPasswordHash(), ci.getSalt(), mode)) {
                        user = null;
                    } else {
                        database.setLastConnectionInfo(ci);
                    }
                }
            }
        }
        if (user == null) {
            throw DbException.get(ErrorCode.WRONG_USER_OR_PASSWORD);
        }
        return user;
    }
}
