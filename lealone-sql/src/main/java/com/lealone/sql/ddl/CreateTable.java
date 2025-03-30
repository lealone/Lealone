/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import java.util.ArrayList;
import java.util.HashSet;

import com.lealone.common.exceptions.ConfigException;
import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.db.Database;
import com.lealone.db.DbObjectType;
import com.lealone.db.DbSetting;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.index.IndexColumn;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.plugin.PluginManager;
import com.lealone.db.schema.Schema;
import com.lealone.db.schema.Sequence;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.table.CreateTableData;
import com.lealone.db.table.Table;
import com.lealone.db.table.TableCodeGenerator;
import com.lealone.db.table.TableSetting;
import com.lealone.db.value.DataType;
import com.lealone.sql.SQLStatement;
import com.lealone.sql.dml.Insert;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.optimizer.TableFilter;
import com.lealone.sql.query.Query;
import com.lealone.storage.StorageSetting;

/**
 * This class represents the statement
 * CREATE TABLE
 * 
 * @author H2 Group
 * @author zhh
 */
public class CreateTable extends SchemaStatement {

    protected final CreateTableData data = new CreateTableData();
    protected IndexColumn[] pkColumns;
    protected boolean ifNotExists;

    private final ArrayList<DefinitionStatement> constraintCommands = new ArrayList<>();
    private boolean onCommitDrop;
    private boolean onCommitTruncate;
    private Query asQuery;
    private String comment;
    private String packageName;
    private boolean genCode;
    private String codePath;
    private String codeGenerator;

    public CreateTable(ServerSession session, Schema schema) {
        super(session, schema);
        data.persistIndexes = true;
        data.persistData = true;
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_TABLE;
    }

    public void setQuery(Query query) {
        this.asQuery = query;
    }

    public void setTemporary(boolean temporary) {
        data.temporary = temporary;
    }

    public void setTableName(String tableName) {
        data.tableName = tableName;
    }

    /**
     * Add a column to this table.
     *
     * @param column the column to add
     */
    public void addColumn(Column column) {
        data.columns.add(column);
    }

    /**
     * Add a constraint statement to this statement.
     * The primary key definition is one possible constraint statement.
     *
     * @param command the statement to add
     */
    public void addConstraintCommand(DefinitionStatement command) {
        if (command instanceof CreateIndex) {
            constraintCommands.add(command);
        } else {
            AlterTableAddConstraint con = (AlterTableAddConstraint) command;
            boolean alreadySet;
            if (con.getType() == SQLStatement.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY) {
                alreadySet = setPrimaryKeyColumns(con.getIndexColumns());
            } else {
                alreadySet = false;
            }
            if (!alreadySet) {
                constraintCommands.add(command);
            }
        }
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    private void validateParameters() {
        CaseInsensitiveMap<String> parameters = new CaseInsensitiveMap<>();
        if (data.storageEngineParams != null)
            parameters.putAll(data.storageEngineParams);
        if (parameters.isEmpty())
            return;

        HashSet<String> recognizedSettingOptions = new HashSet<>(
                StorageSetting.values().length + TableSetting.values().length);
        recognizedSettingOptions.addAll(DbSetting.getRecognizedStorageSetting());
        for (StorageSetting s : StorageSetting.values())
            recognizedSettingOptions.add(s.name());
        for (TableSetting s : TableSetting.values())
            recognizedSettingOptions.add(s.name());

        parameters.removeAll(recognizedSettingOptions);
        if (!parameters.isEmpty()) {
            throw new ConfigException(String.format("Unrecognized parameters: %s for table %s, " //
                    + "recognized setting options: %s", //
                    parameters.keySet(), data.tableName, recognizedSettingOptions));
        }
    }

    @Override
    public int update() {
        validateParameters();
        DbObjectLock lock = schema.tryExclusiveLock(DbObjectType.TABLE_OR_VIEW, session);
        if (lock == null)
            return -1;

        Database db = session.getDatabase();
        if (!db.isPersistent()) {
            data.persistIndexes = false;
        }
        if (schema.findTableOrView(session, data.tableName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, data.tableName);
        }
        if (asQuery != null) {
            asQuery.prepare();
            if (data.columns.isEmpty()) {
                generateColumnsFromQuery();
            } else if (data.columns.size() != asQuery.getColumnCount()) {
                throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
        }
        if (pkColumns != null) {
            for (Column c : data.columns) {
                for (IndexColumn idxCol : pkColumns) {
                    if (c.getName().equals(idxCol.columnName)) {
                        c.setNullable(false);
                    }
                }
            }
        }
        data.id = getObjectId();
        data.create = !session.getDatabase().isStarting();
        data.session = session;
        boolean isSessionTemporary = data.temporary && !data.globalTemporary;
        Table table = schema.createTable(data);
        ArrayList<Sequence> sequences = new ArrayList<>();
        for (Column c : data.columns) {
            if (c.isAutoIncrement()) {
                int objId = getObjectId();
                c.convertAutoIncrementToSequence(session, schema, objId, data.temporary, lock);
            }
            Sequence seq = c.getSequence();
            if (seq != null) {
                sequences.add(seq);
            }
        }
        table.setComment(comment);
        table.setPackageName(packageName);
        table.setCodePath(codePath);
        if (isSessionTemporary) {
            if (onCommitDrop) {
                table.setOnCommitDrop(true);
            }
            if (onCommitTruncate) {
                table.setOnCommitTruncate(true);
            }
            session.addLocalTempTable(table);
        } else {
            schema.add(session, table, lock);
        }
        try {
            TableFilter tf = new TableFilter(session, table, null, false, null);
            for (Column c : data.columns) {
                c.prepareExpression(session, tf);
            }
            for (Sequence sequence : sequences) {
                table.addSequence(sequence);
            }
            for (DefinitionStatement command : constraintCommands) {
                command.update();
            }
            if (asQuery != null) {
                Insert insert = new Insert(session);
                insert.setQuery(asQuery);
                insert.setTable(table);
                insert.prepare();
                insert.update();
            }
        } catch (DbException e) {
            db.checkPowerOff();
            schema.remove(session, table, lock);
            throw e;
        }

        String varName = "disable_generate_code";
        if (session.getDatabase().getSettings().databaseToUpper)
            varName = varName.toUpperCase();
        boolean disableGenCode = session.getVariable(varName).getBoolean();

        // 数据库在启动阶段执行建表语句时不用再生成代码
        if (genCode && !session.getDatabase().isStarting() && !disableGenCode) {
            if (codeGenerator == null)
                codeGenerator = "default_table_code_generator"; // 采用ORM框架的默认值，兼容老版本
            TableCodeGenerator tGenerator = PluginManager.getPlugin(TableCodeGenerator.class,
                    codeGenerator);
            if (tGenerator == null)
                throw DbException.get(ErrorCode.PLUGIN_NOT_FOUND_1, codeGenerator);
            tGenerator.genCode(session, table, table, 1);
        }
        return 0;
    }

    private void generateColumnsFromQuery() {
        int columnCount = asQuery.getColumnCount();
        ArrayList<Expression> expressions = asQuery.getExpressions();
        for (int i = 0; i < columnCount; i++) {
            Expression expr = expressions.get(i);
            int type = expr.getType();
            String name = expr.getAlias();
            long precision = expr.getPrecision();
            int displaySize = expr.getDisplaySize();
            DataType dt = DataType.getDataType(type);
            if (precision > 0 && //
                    (dt.defaultPrecision == 0 //
                            || (dt.defaultPrecision > precision
                                    && dt.defaultPrecision < Byte.MAX_VALUE))) {
                // dont' set precision to MAX_VALUE if this is the default
                precision = dt.defaultPrecision;
            }
            int scale = expr.getScale();
            if (scale > 0 && (dt.defaultScale == 0
                    || (dt.defaultScale > scale && dt.defaultScale < precision))) {
                scale = dt.defaultScale;
            }
            if (scale > precision) {
                precision = scale;
            }
            Column col = new Column(name, type, precision, scale, displaySize);
            addColumn(col);
        }
    }

    /**
     * Sets the primary key columns, but also check if a primary key
     * with different columns is already defined.
     *
     * @param columns the primary key columns
     * @return true if the same primary key columns where already set
     */
    private boolean setPrimaryKeyColumns(IndexColumn[] columns) {
        if (pkColumns != null) {
            int len = columns.length;
            if (len != pkColumns.length) {
                throw DbException.get(ErrorCode.SECOND_PRIMARY_KEY);
            }
            for (int i = 0; i < len; i++) {
                if (!columns[i].columnName.equals(pkColumns[i].columnName)) {
                    throw DbException.get(ErrorCode.SECOND_PRIMARY_KEY);
                }
            }
            return true;
        }
        this.pkColumns = columns;
        return false;
    }

    public void setPersistIndexes(boolean persistIndexes) {
        data.persistIndexes = persistIndexes;
    }

    public void setPersistData(boolean persistData) {
        data.persistData = persistData;
        if (!persistData) {
            data.persistIndexes = false;
        }
    }

    public void setGlobalTemporary(boolean globalTemporary) {
        data.globalTemporary = globalTemporary;
    }

    public void setHidden(boolean isHidden) {
        data.isHidden = isHidden;
    }

    /**
     * This temporary table is dropped on commit.
     */
    public void setOnCommitDrop() {
        this.onCommitDrop = true;
    }

    /**
     * This temporary table is truncated on commit.
     */
    public void setOnCommitTruncate() {
        this.onCommitTruncate = true;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setStorageEngineName(String storageEngineName) {
        data.storageEngineName = storageEngineName;
    }

    public void setStorageEngineParams(CaseInsensitiveMap<String> storageEngineParams) {
        data.storageEngineParams = storageEngineParams;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setGenCode(boolean genCode) {
        this.genCode = genCode;
    }

    public boolean isGenCode() {
        return genCode;
    }

    public void setCodePath(String codePath) {
        this.codePath = codePath;
    }

    public String getCodePath() {
        return codePath;
    }

    public void setCodeGenerator(String codeGenerator) {
        this.codeGenerator = codeGenerator;
    }

    public String getCodeGenerator() {
        return codeGenerator;
    }
}
