/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.session.ServerSession;
import org.lealone.net.NetNodeManagerHolder;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * CREATE DATABASE
 */
public class CreateDatabase extends DatabaseStatement {

    private final boolean ifNotExists;

    public CreateDatabase(ServerSession session, String dbName, boolean ifNotExists, RunMode runMode,
            CaseInsensitiveMap<String> parameters) {
        super(session, dbName);
        this.ifNotExists = ifNotExists;
        this.runMode = runMode;
        this.parameters = parameters;
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_DATABASE;
    }

    @Override
    public boolean isIfDDL() {
        return ifNotExists;
    }

    @Override
    public int update() {
        checkRight(ErrorCode.CREATE_DATABASE_RIGHTS_REQUIRED);
        LealoneDatabase lealoneDB = LealoneDatabase.getInstance();
        DbObjectLock lock = lealoneDB.tryExclusiveDatabaseLock(session);
        if (lock == null)
            return -1;

        if (lealoneDB.findDatabase(dbName) != null || LealoneDatabase.NAME.equalsIgnoreCase(dbName)) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.DATABASE_ALREADY_EXISTS_1, dbName);
        }
        validateParameters();
        int id = getObjectId(lealoneDB);
        Database newDB = new Database(id, dbName, parameters);
        newDB.setReplicationParameters(replicationParameters);
        newDB.setNodeAssignmentParameters(nodeAssignmentParameters);
        newDB.setRunMode(runMode);
        if (!parameters.containsKey("hostIds")) {
            String[] hostIds = NetNodeManagerHolder.get().assignNodes(newDB);
            newDB.setHostIds(hostIds);
        }
        if (newDB.getHostIds().length <= 1) {
            // 如果可用节点只有1个，那就退化到CLIENT_SERVER模式
            newDB.setRunMode(RunMode.CLIENT_SERVER);
        }
        lealoneDB.addDatabaseObject(session, newDB, lock);
        // 将缓存过期掉
        lealoneDB.getNextModificationMetaId();

        // LealoneDatabase在启动过程中执行CREATE DATABASE时，不对数据库初始化
        if (!lealoneDB.isStarting()) {
            // 不能直接使用sql字段，因为parameters有可能不一样，比如额外加了hostIds
            updateRemoteNodes(newDB.getCreateSQL());
            // 只有数据库真实所在的目标节点才需要初始化数据库，其他节点只需要在LealoneDatabase中有一条相应记录即可
            if (isTargetNode(newDB)) {
                newDB.init();
                newDB.createRootUserIfNotExists();
            }
        }
        return 0;
    }
}
