/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.ddl;

import java.util.HashSet;

import com.lealone.common.exceptions.ConfigException;
import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.db.DbSetting;
import com.lealone.db.session.ServerSession;

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

    public String getDatabaseName() {
        return dbName;
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
                    + "recognized database setting options: %s", //
                    parameters.keySet(), dbName, recognizedSettingOptions));
        }
    }
}
