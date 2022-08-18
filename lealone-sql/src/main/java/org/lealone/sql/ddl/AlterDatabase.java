/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.ddl;

import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * ALTER DATABASE
 */
public class AlterDatabase extends DatabaseStatement {

    private final Database db;
    private String hostIds; // 可以指定具体的hostId

    public AlterDatabase(ServerSession session, Database db, RunMode runMode,
            CaseInsensitiveMap<String> parameters) {
        super(session, db.getName());
        this.db = db;
        this.runMode = runMode;
        // 先使用原有的参数，然后再用新的覆盖
        this.parameters = new CaseInsensitiveMap<>(db.getParameters());
        if (parameters != null && !parameters.isEmpty()) {
            hostIds = parameters.get("hostIds");
            if (hostIds != null) {
                hostIds = hostIds.trim();
                if (hostIds.isEmpty())
                    hostIds = null;
            }

            this.parameters.putAll(parameters);
            validateParameters();
        }
    }

    @Override
    public int getType() {
        return SQLStatement.ALTER_DATABASE;
    }

    @Override
    public int update() {
        checkRight();
        LealoneDatabase lealoneDB = LealoneDatabase.getInstance();
        DbObjectLock lock = lealoneDB.tryExclusiveDatabaseLock(session);
        if (lock == null)
            return -1;

        RunMode oldRunMode = db.getRunMode();
        if (runMode == null) {
            runMode = oldRunMode;
        }
        alterDatabase();
        updateLocalMeta();
        return 0;
    }

    private void alterDatabase() {
        if (runMode != null)
            db.setRunMode(runMode);
        if (parameters != null)
            db.alterParameters(parameters);
        if (replicationParameters != null)
            db.setReplicationParameters(replicationParameters);
        if (nodeAssignmentParameters != null)
            db.setNodeAssignmentParameters(nodeAssignmentParameters);
    }

    private void updateLocalMeta() {
        LealoneDatabase.getInstance().updateMeta(session, db);
    }
}
