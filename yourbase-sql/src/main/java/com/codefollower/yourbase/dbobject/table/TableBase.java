/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.yourbase.dbobject.table;

import com.codefollower.yourbase.command.ddl.CreateTableData;
import com.codefollower.yourbase.dbobject.Schema;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.result.Row;
import com.codefollower.yourbase.util.StatementBuilder;
import com.codefollower.yourbase.util.StringUtils;

/**
 * The base class of a regular table, or a user defined table.
 *
 * @author Thomas Mueller
 * @author Sergi Vladykin
 */
public abstract class TableBase extends Table {
    private final boolean globalTemporary;
    protected boolean containsLargeObject;
    protected long rowCount;

    public TableBase(Schema schema, int id, String name, boolean persistIndexes, boolean persistData, String tableEngine,
            boolean globalTemporary) {
        super(schema, id, name, persistIndexes, persistData);
        this.tableEngine = tableEngine;
        this.globalTemporary = globalTemporary;
    }

    public TableBase(CreateTableData data) {
        super(data.schema, data.id, data.tableName, data.persistIndexes, data.persistData);
        this.tableEngine = data.tableEngine;
        this.globalTemporary = data.globalTemporary;
        setTemporary(data.temporary);
        Column[] cols = new Column[data.columns.size()];
        data.columns.toArray(cols);
        setColumns(cols);
    }

    public boolean getContainsLargeObject() {
        return containsLargeObject;
    }

    /**
     * Set the row count of this table.
     *
     * @param count the row count
     */
    public void setRowCount(long count) {
        this.rowCount = count;
    }
    
    /**
     * Read the given row.
     *
     * @param session the session
     * @param key unique key
     * @return the row
     */
    public abstract Row getRow(Session session, long key);

    public String getDropSQL() {
        return "DROP TABLE IF EXISTS " + getSQL() + " CASCADE";
    }

    public String getCreateSQL() {
        StatementBuilder buff = new StatementBuilder("CREATE ");
        if (isTemporary()) {
            if (isGlobalTemporary()) {
                buff.append("GLOBAL ");
            } else {
                buff.append("LOCAL ");
            }
            buff.append("TEMPORARY ");
        } else if (isPersistIndexes()) {
            buff.append("CACHED ");
        } else {
            buff.append("MEMORY ");
        }
        buff.append("TABLE ");
        if (isHidden) {
            buff.append("IF NOT EXISTS ");
        }
        buff.append(getSQL());
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        buff.append("(\n    ");
        for (Column column : columns) {
            buff.appendExceptFirst(",\n    ");
            buff.append(column.getCreateSQL());
        }
        buff.append("\n)");
        if (tableEngine != null) {
            String d = getDatabase().getSettings().defaultTableEngine;
            if (d == null || !tableEngine.endsWith(d)) {
                buff.append("\nENGINE \"");
                buff.append(tableEngine);
                buff.append('\"');
            }
        }
        if (!isPersistIndexes() && !isPersistData()) {
            buff.append("\nNOT PERSISTENT");
        }
        if (isHidden) {
            buff.append("\nHIDDEN");
        }
        return buff.toString();
    }

    public boolean isGlobalTemporary() {
        return globalTemporary;
    }

}
