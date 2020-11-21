/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CamelCaseHelper;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.db.Database;
import org.lealone.db.DbObjectType;
import org.lealone.db.ProcessingMode;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.constraint.Constraint;
import org.lealone.db.constraint.ConstraintReferential;
import org.lealone.db.index.IndexColumn;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.schema.Schema;
import org.lealone.db.schema.Sequence;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.CreateTableData;
import org.lealone.db.table.Table;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.dml.Insert;
import org.lealone.sql.dml.Query;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.optimizer.TableFilter;

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
    private ProcessingMode processingMode;

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

    @Override
    public int update() {
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
        data.create = create;
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
        table.setProcessingMode(processingMode);
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
                insert.setInsertFromSelect(true);
                insert.prepare();
                insert.update();
            }
        } catch (DbException e) {
            db.checkPowerOff();
            schema.remove(session, table, lock);
            throw e;
        }

        // 数据库在启动阶段执行建表语句时不用再生成代码
        if (genCode && !session.getDatabase().isStarting())
            genCode(session, table, table, 1);
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
                            || (dt.defaultPrecision > precision && dt.defaultPrecision < Byte.MAX_VALUE))) {
                // dont' set precision to MAX_VALUE if this is the default
                precision = dt.defaultPrecision;
            }
            int scale = expr.getScale();
            if (scale > 0 && (dt.defaultScale == 0 || (dt.defaultScale > scale && dt.defaultScale < precision))) {
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

    public void setProcessingMode(ProcessingMode mode) {
        processingMode = mode;
    }

    public void setStorageEngineName(String storageEngineName) {
        data.storageEngineName = storageEngineName;
    }

    public void setStorageEngineParams(CaseInsensitiveMap<String> storageEngineParams) {
        data.storageEngineParams = storageEngineParams;
    }

    @Override
    public boolean isReplicationStatement() {
        return true;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public void setGenCode(boolean genCode) {
        this.genCode = genCode;
    }

    public void setCodePath(String codePath) {
        this.codePath = codePath;
    }

    public boolean isGenCode() {
        return genCode;
    }

    public String getCodePath() {
        return codePath;
    }

    // table.getConstraints()可能返回null
    private static ArrayList<Constraint> getConstraints(Table table) {
        ArrayList<Constraint> constraints = table.getConstraints();
        if (constraints == null)
            constraints = new ArrayList<>(0);
        return constraints;
    }

    private static void genCode(ServerSession session, Table table, Table owner, int level) {
        String packageName = table.getPackageName();
        String tableName = table.getName();
        Schema schema = table.getSchema();
        for (Constraint constraint : getConstraints(table)) {
            if (constraint instanceof ConstraintReferential) {
                ConstraintReferential ref = (ConstraintReferential) constraint;
                Table refTable = ref.getRefTable();
                if (refTable != table && level <= 1) { // 避免递归
                    genCode(session, refTable, owner, ++level);
                }
            }
        }
        boolean databaseToUpper = session.getDatabase().getSettings().databaseToUpper;
        String className = CreateService.toClassName(tableName);
        StringBuilder buff = new StringBuilder();
        StringBuilder fields = new StringBuilder();
        StringBuilder fieldNames = new StringBuilder();
        StringBuilder init = new StringBuilder();
        TreeSet<String> importSet = new TreeSet<>();

        importSet.add("org.lealone.orm.Model");
        importSet.add("org.lealone.orm.ModelDeserializer");
        importSet.add("org.lealone.orm.ModelSerializer");
        importSet.add("org.lealone.orm.ModelTable");
        importSet.add("org.lealone.orm.ModelProperty");
        importSet.add("com.fasterxml.jackson.databind.annotation.JsonDeserialize");
        importSet.add("com.fasterxml.jackson.databind.annotation.JsonSerialize");
        importSet.add(packageName + "." + className + "." + className + "Deserializer");

        for (Constraint constraint : getConstraints(table)) {
            if (constraint instanceof ConstraintReferential) {
                ConstraintReferential ref = (ConstraintReferential) constraint;
                Table refTable = ref.getRefTable();
                owner = ref.getTable();
                if (refTable == table) {
                    String pn = owner.getPackageName();
                    if (!packageName.equals(pn)) {
                        importSet.add(pn + "." + CreateService.toClassName(owner.getName()));
                    }
                    importSet.add(List.class.getName());
                    importSet.add(ArrayList.class.getName());
                } else {
                    String pn = refTable.getPackageName();
                    if (!packageName.equals(pn)) {
                        importSet.add(pn + "." + CreateService.toClassName(refTable.getName()));
                    }
                }
            }
        }

        for (Column c : table.getColumns()) {
            int type = c.getType();
            String modelPropertyClassName = getModelPropertyClassName(type, importSet);
            String columnName = CamelCaseHelper.toCamelFromUnderscore(c.getName());

            fields.append("    public final ").append(modelPropertyClassName).append('<').append(className).append("> ")
                    .append(columnName).append(";\r\n");

            // 例如: this.id = new PLong<>("id", this);
            init.append("        this.").append(columnName).append(" = new ").append(modelPropertyClassName)
                    .append("<>(\"").append(databaseToUpper ? c.getName().toUpperCase() : c.getName())
                    .append("\", this);\r\n");

            if (fieldNames.length() > 0) {
                fieldNames.append(", ");
            }
            fieldNames.append("this.").append(columnName);
        }

        buff.append("package ").append(packageName).append(";\r\n\r\n");
        for (String p : importSet) {
            buff.append("import ").append(p).append(";\r\n");
        }
        buff.append("\r\n");
        buff.append("/**\r\n");
        buff.append(" * Model for table '").append(tableName).append("'.\r\n");
        buff.append(" *\r\n");
        buff.append(" * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.\r\n");
        buff.append(" */\r\n");
        buff.append("@JsonSerialize(using = ModelSerializer.class)\r\n");
        buff.append("@JsonDeserialize(using = ").append(className).append("Deserializer.class)\r\n");
        // 例如: public class Customer extends Model<Customer> {
        buff.append("public class ").append(className).append(" extends Model<").append(className).append("> {\r\n");
        buff.append("\r\n");

        buff.append("    public static final ").append(className).append(" dao = new ").append(className)
                .append("(null, ROOT_DAO);\r\n");
        buff.append("\r\n");
        Database db = schema.getDatabase();
        String tableFullName = "\"" + db.getName() + "\", \"" + schema.getName() + "\", \"" + tableName + "\"";
        if (db.getSettings().databaseToUpper) {
            tableFullName = tableFullName.toUpperCase();
        }
        // buff.append(" public static ").append(className).append(" create(String url) {\r\n");
        // buff.append(" ModelTable t = new ModelTable(url, ").append(tableFullName).append(");\r\n");
        // buff.append(" return new ").append(className).append("(t, REGULAR_MODEL);\r\n");
        // buff.append(" }\r\n");
        // buff.append("\r\n");
        buff.append(fields);
        // buff.append("\r\n");

        StringBuilder listBuff = new StringBuilder();
        StringBuilder newAssociateInstanceBuff = new StringBuilder();
        int mVar = 0;
        for (Constraint constraint : getConstraints(table)) {
            if (constraint instanceof ConstraintReferential) {
                ConstraintReferential ref = (ConstraintReferential) constraint;
                Table refTable = ref.getRefTable();
                owner = ref.getTable();
                String refTableClassName = CreateService.toClassName(refTable.getName());
                if (refTable == table) {
                    mVar++;
                    String ownerClassName = CreateService.toClassName(owner.getName());

                    listBuff.append("    public ").append(className).append(" add").append(ownerClassName).append("(")
                            .append(ownerClassName).append(" m) {\r\n");
                    listBuff.append("        m.set").append(refTableClassName).append("(this);\r\n");
                    listBuff.append("        super.addModel(m);\r\n");
                    listBuff.append("        return this;\r\n");
                    listBuff.append("    }\r\n");
                    listBuff.append("\r\n");
                    listBuff.append("    public ").append(className).append(" add").append(ownerClassName).append("(")
                            .append(ownerClassName).append("... mArray) {\r\n");
                    listBuff.append("        for (").append(ownerClassName).append(" m : mArray)\r\n");
                    listBuff.append("            add").append(ownerClassName).append("(m);\r\n");
                    listBuff.append("        return this;\r\n");
                    listBuff.append("    }\r\n");
                    listBuff.append("\r\n");
                    listBuff.append("    public List<").append(ownerClassName).append("> get").append(ownerClassName)
                            .append("List() {\r\n");
                    listBuff.append("        return super.getModelList(").append(ownerClassName).append(".class);\r\n");
                    listBuff.append("    }\r\n");
                    listBuff.append("\r\n");
                    // newAssociateInstanceBuff.append(" @Override\r\n");
                    // newAssociateInstanceBuff.append(" protected ").append(ownerClassName)
                    // .append(" newAssociateInstance() {\r\n");
                    // newAssociateInstanceBuff.append(" ").append(ownerClassName).append(" m = new ")
                    // .append(ownerClassName).append("();\r\n");
                    // newAssociateInstanceBuff.append(" add").append(ownerClassName).append("(m);\r\n");
                    // newAssociateInstanceBuff.append(" return m;\r\n");
                    // newAssociateInstanceBuff.append(" }\r\n");
                    // newAssociateInstanceBuff.append("\r\n");

                    String m = "m" + mVar;
                    newAssociateInstanceBuff.append("        ").append(ownerClassName).append(" ").append(m)
                            .append(" = new ").append(ownerClassName).append("();\r\n");
                    newAssociateInstanceBuff.append("        add").append(ownerClassName).append("(").append(m)
                            .append(");\r\n");
                    newAssociateInstanceBuff.append("        list.add").append("(").append(m).append(");\r\n");
                } else {
                    String refTableVar = CamelCaseHelper.toCamelFromUnderscore(refTable.getName());

                    buff.append("    private ").append(refTableClassName).append(" ").append(refTableVar)
                            .append(";\r\n");
                    listBuff.append("    public ").append(refTableClassName).append(" get").append(refTableClassName)
                            .append("() {\r\n");
                    listBuff.append("        return ").append(refTableVar).append(";\r\n");
                    listBuff.append("    }\r\n");
                    listBuff.append("\r\n");
                    listBuff.append("    public ").append(className).append(" set").append(refTableClassName)
                            .append("(").append(refTableClassName).append(" ").append(refTableVar).append(") {\r\n");
                    listBuff.append("        this.").append(refTableVar).append(" = ").append(refTableVar)
                            .append(";\r\n");

                    IndexColumn[] refColumns = ref.getRefColumns();
                    IndexColumn[] columns = ref.getColumns();
                    for (int i = 0; i < columns.length; i++) {
                        String columnName = CamelCaseHelper.toCamelFromUnderscore(columns[i].column.getName());
                        String refColumnName = CamelCaseHelper.toCamelFromUnderscore(refColumns[i].column.getName());
                        listBuff.append("        this.").append(columnName).append(".set(").append(refTableVar)
                                .append(".").append(refColumnName).append(".get());\r\n");
                    }
                    listBuff.append("        return this;\r\n");
                    listBuff.append("    }\r\n");
                    listBuff.append("\r\n");
                }
            }
        }

        buff.append("\r\n");
        buff.append("    public ").append(className).append("() {\r\n");
        buff.append("        this(null, REGULAR_MODEL);\r\n");
        buff.append("    }\r\n");
        buff.append("\r\n");
        buff.append("    private ").append(className).append("(ModelTable t, short modelType) {\r\n");
        buff.append("        super(t == null ? new ModelTable(").append(tableFullName).append(") : t, modelType);\r\n");
        buff.append("        super.setRoot(this);\r\n");
        buff.append("\r\n");
        buff.append(init);
        buff.append("        super.setModelProperties(new ModelProperty[] { ").append(fieldNames).append(" });\r\n");
        buff.append("    }\r\n");
        buff.append("\r\n");
        buff.append(listBuff);
        buff.append("    @Override\r\n");
        buff.append("    protected ").append(className).append(" newInstance(ModelTable t, short modelType) {\r\n");
        buff.append("        return new ").append(className).append("(t, modelType);\r\n");
        buff.append("    }\r\n");
        buff.append("\r\n");
        if (newAssociateInstanceBuff.length() > 0) {
            buff.append("    @Override\r\n");
            buff.append("    protected List<Model<?>> newAssociateInstances() {\r\n");
            buff.append("        ArrayList<Model<?>> list = new ArrayList<>();\r\n");
            buff.append(newAssociateInstanceBuff);
            buff.append("        return list;\r\n");
            buff.append("    }\r\n");
            buff.append("\r\n");
        }
        buff.append("    static class ").append(className).append("Deserializer extends ModelDeserializer<")
                .append(className).append("> {\r\n");
        buff.append("        @Override\r\n");
        buff.append("        protected Model<").append(className).append("> newModelInstance() {\r\n");
        buff.append("            return new ").append(className).append("();\r\n");
        buff.append("        }\r\n");
        buff.append("    }\r\n");
        buff.append("}\r\n");
        // System.out.println(buff);

        CreateService.writeFile(table.getCodePath(), packageName, className, buff);
    }

    private static final String TYPE_QUERY_PACKAGE_NAME = "org.lealone.orm.property";

    private static String getModelPropertyClassName(int type, TreeSet<String> importSet) {
        String name;
        switch (type) {
        case Value.BYTES:
            name = "Bytes";
            break;
        case Value.UUID:
            name = "Uuid";
            break;
        case Value.NULL:
            throw DbException.throwInternalError("type = null");
        default:
            name = DataType.getTypeClassName(type);
            int pos = name.lastIndexOf('.');
            name = name.substring(pos + 1);
        }
        name = "P" + name;
        importSet.add(TYPE_QUERY_PACKAGE_NAME + "." + name);
        return name;
    }

    // private static String getTypeClassName(int type, TreeSet<String> importSet) {
    // switch (type) {
    // case Value.BYTES:
    // return "byte[]";
    // case Value.UUID:
    // importSet.add(UUID.class.getName());
    // return UUID.class.getSimpleName();
    // case Value.NULL:
    // throw DbException.throwInternalError("type = null");
    // default:
    // String name = DataType.getTypeClassName(type);
    // if (!name.startsWith("java.lang.")) {
    // importSet.add(name);
    // }
    // return name.substring(name.lastIndexOf('.') + 1);
    // }
    // }
}
