/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.ddl;

import java.util.HashSet;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.db.DbSetting;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.session.ServerSession;

//CREATE/ALTER/DROP DATABASE语句在所有节点上都会执行一次，
//差别是数据库所在节点会执行更多操作，其他节点只在LealoneDatabase中有一条相应记录，
//这样客户端在接入集群的任何节点时都能找到所连数据库所在的节点有哪些。
public abstract class DatabaseStatement extends DefinitionStatement {

    protected final String dbName;
    protected RunMode runMode;
    protected CaseInsensitiveMap<String> parameters;
    protected CaseInsensitiveMap<String> replicationParameters;
    protected CaseInsensitiveMap<String> nodeAssignmentParameters;

    protected DatabaseStatement(ServerSession session, String dbName) {
        super(session);
        this.dbName = dbName;
    }

    @Override
    public boolean isDatabaseStatement() {
        return true;
    }

    protected void checkRight() {
        checkRight(null);
    }

    protected void checkRight(Integer errorCode) {
        // 只有用管理员连接到LealoneDatabase才能执行CREATE/ALTER/DROP DATABASE语句
        if (!(LealoneDatabase.getInstance() == session.getDatabase() && session.getUser().isAdmin())) {
            if (errorCode != null)
                throw DbException.get(errorCode.intValue());
            else
                throw DbException.get(ErrorCode.GENERAL_ERROR_1,
                        "create/alter/drop database only allowed for the super user");
        }
    }

    protected void validateParameters() {
        if (this.parameters == null)
            this.parameters = new CaseInsensitiveMap<>();

        // 第一步: 验证可识别的参数
        CaseInsensitiveMap<String> parameters = new CaseInsensitiveMap<>(this.parameters);
        removeParameters(parameters);

        HashSet<String> recognizedSettingOptions = new HashSet<>(DbSetting.values().length);
        for (DbSetting s : DbSetting.values())
            recognizedSettingOptions.add(s.name());

        parameters.removeAll(recognizedSettingOptions);
        if (!parameters.isEmpty()) {
            throw new ConfigException(String.format("Unrecognized parameters: %s for database %s, " //
                    + "database setting options: %s", //
                    parameters.keySet(), dbName, recognizedSettingOptions));
        }

        // 第二步: 初始化replicationParameters、nodeAssignmentParameters、database settings
        parameters = new CaseInsensitiveMap<>(this.parameters);
        removeParameters(parameters);

        // parameters剩下的当成database setting
    }

    private static void removeParameters(CaseInsensitiveMap<String> parameters) {
        parameters.remove("hostIds");
        parameters.remove("replication_strategy");
        parameters.remove("node_assignment_strategy");
        parameters.remove("_operationNode_");
        parameters.remove("_removeHostIds_");
    }
}
