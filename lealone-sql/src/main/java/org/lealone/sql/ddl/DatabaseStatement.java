/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.ddl;

import java.util.HashSet;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.db.DbSetting;
import org.lealone.db.session.ServerSession;

//CREATE/ALTER/DROP DATABASE语句在所有节点上都会执行一次，
//差别是数据库所在节点会执行更多操作，其他节点只在LealoneDatabase中有一条相应记录，
//这样客户端在接入集群的任何节点时都能找到所连数据库所在的节点有哪些。
public abstract class DatabaseStatement extends DefinitionStatement {

    protected final String dbName;
    protected CaseInsensitiveMap<String> parameters;

    protected DatabaseStatement(ServerSession session, String dbName) {
        super(session);
        this.dbName = dbName;
    }

    @Override
    public boolean isDatabaseStatement() {
        return true;
    }

    protected void validateParameters() {
        if (parameters == null || parameters.isEmpty())
            return;
        CaseInsensitiveMap<String> parameters = new CaseInsensitiveMap<>(this.parameters);
        HashSet<String> recognizedSettingOptions = new HashSet<>(DbSetting.values().length);
        for (DbSetting s : DbSetting.values())
            recognizedSettingOptions.add(s.name());

        parameters.removeAll(recognizedSettingOptions);
        if (!parameters.isEmpty()) {
            throw new ConfigException(String.format("Unrecognized parameters: %s for database %s, " //
                    + "database setting options: %s", //
                    parameters.keySet(), dbName, recognizedSettingOptions));
        }
    }
}
