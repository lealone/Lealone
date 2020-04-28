/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StatementBuilder;
import org.lealone.common.util.Utils;
import org.lealone.db.Database;
import org.lealone.db.DbObject;
import org.lealone.db.DbObjectBase;
import org.lealone.db.DbObjectType;
import org.lealone.db.SysProperties;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.auth.User;
import org.lealone.db.constraint.Constraint;
import org.lealone.db.index.Index;
import org.lealone.db.service.Service;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.CreateTableData;
import org.lealone.db.table.StandardTable;
import org.lealone.db.table.Table;
import org.lealone.db.table.TableFactory;
import org.lealone.storage.StorageEngine;
import org.lealone.storage.StorageEngineManager;
import org.lealone.storage.memory.MemoryStorageEngine;

/**
 * A schema as created by the SQL statement
 * CREATE SCHEMA
 *
 * @author H2 Group
 * @author zhh
 */
public class Schema extends DbObjectBase {

    /**
     * The set of returned unique names that are not yet stored. It is used to
     * avoid returning the same unique name twice when multiple threads
     * concurrently create objects.
     */
    private final HashSet<String> temporaryUniqueNames = new HashSet<>();

    private final HashMap<String, Table> tablesAndViews;
    private final HashMap<String, Index> indexes;
    private final HashMap<String, Sequence> sequences;
    private final HashMap<String, TriggerObject> triggers;
    private final HashMap<String, Constraint> constraints;
    private final HashMap<String, Constant> constants;
    private final HashMap<String, FunctionAlias> functions;
    private final HashMap<String, UserAggregate> aggregates;
    private final HashMap<String, UserDataType> userDataTypes;
    private final HashMap<String, Service> services;

    private final boolean system;
    private User owner;

    /**
     * Create a new schema object.
     *
     * @param database the database
     * @param id the object id
     * @param schemaName the schema name
     * @param owner the owner of the schema
     * @param system if this is a system schema (such a schema can not be dropped)
     */
    public Schema(Database database, int id, String schemaName, User owner, boolean system) {
        super(database, id, schemaName);
        tablesAndViews = database.newStringMap();
        indexes = database.newStringMap();
        sequences = database.newStringMap();
        triggers = database.newStringMap();
        constraints = database.newStringMap();
        constants = database.newStringMap();
        functions = database.newStringMap();
        aggregates = database.newStringMap();
        userDataTypes = database.newStringMap();
        services = database.newStringMap();
        this.owner = owner;
        this.system = system;
    }

    @Override
    public DbObjectType getType() {
        return DbObjectType.SCHEMA;
    }

    /**
     * Get the owner of this schema.
     *
     * @return the owner
     */
    public User getOwner() {
        return owner;
    }

    /**
     * Check if this schema can be dropped. System schemas can not be dropped.
     *
     * @return true if it can be dropped
     */
    public boolean canDrop() {
        return !system;
    }

    @Override
    public String getCreateSQL() {
        if (system) {
            return null;
        }
        StatementBuilder sql = new StatementBuilder();
        sql.append("CREATE SCHEMA IF NOT EXISTS ").append(getSQL()).append(" AUTHORIZATION ").append(owner.getSQL());
        return sql.toString();
    }

    @Override
    public List<? extends DbObject> getChildren() {
        return getAll();
    }

    @Override
    public void removeChildrenAndResources(ServerSession session) {
        // 删除顺序不能乱，因为可能有依赖
        removeSchemaObjects(session, triggers);
        removeSchemaObjects(session, constraints);

        // There can be dependencies between tables e.g. using computed columns,
        // so we might need to loop over them multiple times.
        boolean runLoopAgain = false;
        do {
            runLoopAgain = false;
            if (tablesAndViews != null) {
                // Loop over a copy because the map is modified underneath us.
                for (Table obj : new ArrayList<>(tablesAndViews.values())) {
                    // Check for null because multiple tables might be deleted
                    // in one go underneath us.
                    if (obj.getName() != null) {
                        if (database.getDependentTable(obj, obj) == null) {
                            obj.getSchema().remove(session, obj);
                        } else {
                            runLoopAgain = true;
                        }
                    }
                }
            }
        } while (runLoopAgain);

        removeSchemaObjects(session, indexes);
        removeSchemaObjects(session, sequences);
        removeSchemaObjects(session, constants);
        removeSchemaObjects(session, functions);
        removeSchemaObjects(session, aggregates);
        removeSchemaObjects(session, userDataTypes);
        removeSchemaObjects(session, services);
        owner = null;
        super.removeChildrenAndResources(session);
    }

    private void removeSchemaObjects(ServerSession session, HashMap<String, ? extends SchemaObject> objs) {
        while (objs != null && objs.size() > 0) {
            SchemaObject obj = (SchemaObject) objs.values().toArray()[0];
            remove(session, obj);
        }
    }

    @SuppressWarnings("unchecked")
    private HashMap<String, SchemaObject> getMap(DbObjectType type) {
        HashMap<String, ? extends SchemaObject> result;
        switch (type) {
        case TABLE_OR_VIEW:
            result = tablesAndViews;
            break;
        case SEQUENCE:
            result = sequences;
            break;
        case INDEX:
            result = indexes;
            break;
        case TRIGGER:
            result = triggers;
            break;
        case CONSTRAINT:
            result = constraints;
            break;
        case CONSTANT:
            result = constants;
            break;
        case FUNCTION_ALIAS:
            result = functions;
            break;
        case AGGREGATE:
            result = aggregates;
            break;
        case USER_DATATYPE:
            result = userDataTypes;
            break;
        case SERVICE:
            result = services;
            break;
        default:
            throw DbException.throwInternalError("type=" + type);
        }
        return (HashMap<String, SchemaObject>) result;
    }

    public Object getLock(DbObjectType type) {
        return getMap(type);
    }

    /**
     * Add an object to this schema.
     *
     * @param obj the object to add
     */
    // 执行DDL语句时session不为null，需要在meta表中增加一条对应的记录
    public void add(ServerSession session, SchemaObject obj) {
        if (SysProperties.CHECK && obj.getSchema() != this) {
            DbException.throwInternalError("wrong schema");
        }
        String name = obj.getName();
        DbObjectType type = obj.getType();
        synchronized (getLock(type)) {
            HashMap<String, SchemaObject> map = getMap(type);
            if (SysProperties.CHECK && map.get(name) != null) {
                DbException.throwInternalError("object already exists: " + name);
            }
            // 先执行addMeta再执行put，因为addMeta可能会失败
            if (session != null)
                database.addMeta(session, obj);
            map.put(name, obj);
            freeUniqueName(name);
        }
    }

    /**
     * Remove an object from this schema.
     *
     * @param obj the object to remove
     */
    public void remove(ServerSession session, SchemaObject obj) {
        String objName = obj.getName();
        DbObjectType type = obj.getType();
        synchronized (getLock(type)) {
            if (session != null && removeLocalTempSchemaObject(session, obj)) {
                freeUniqueName(objName);
                obj.invalidate();
            } else {
                HashMap<String, SchemaObject> map = getMap(type);
                if (SysProperties.CHECK && !map.containsKey(objName)) {
                    DbException.throwInternalError("not found: " + objName);
                }
                if (session != null) {
                    obj.removeChildrenAndResources(session);
                    database.removeMeta(session, obj);
                }
                map.remove(objName);
                freeUniqueName(objName);
                obj.invalidate();
            }
        }
    }

    private boolean removeLocalTempSchemaObject(ServerSession session, SchemaObject obj) {
        DbObjectType type = obj.getType();
        if (type == DbObjectType.TABLE_OR_VIEW) {
            Table table = (Table) obj;
            if (table.isTemporary() && !table.isGlobalTemporary()) {
                session.removeLocalTempTable(table);
                return true;
            }
        } else if (type == DbObjectType.INDEX) {
            Index index = (Index) obj;
            Table table = index.getTable();
            if (table.isTemporary() && !table.isGlobalTemporary()) {
                session.removeLocalTempTableIndex(index);
                return true;
            }
        } else if (type == DbObjectType.CONSTRAINT) {
            Constraint constraint = (Constraint) obj;
            Table table = constraint.getTable();
            if (table.isTemporary() && !table.isGlobalTemporary()) {
                session.removeLocalTempTableConstraint(constraint);
                return true;
            }
        }
        return false;
    }

    /**
     * Rename a schema object.
     *
     * @param session the session
     * @param obj the object to rename
     * @param newName the new name
     */
    public void rename(ServerSession session, SchemaObject obj, String newName) {
        DbObjectType type = obj.getType();
        synchronized (getLock(type)) {
            HashMap<String, SchemaObject> map = getMap(type);
            String oldName = obj.getName();
            if (SysProperties.CHECK) {
                if (!map.containsKey(oldName)) {
                    DbException.throwInternalError("not found: " + oldName);
                }
                if (oldName.equals(newName) || map.containsKey(newName)) {
                    DbException.throwInternalError("object already exists: " + newName);
                }
            }
            if (session != null)
                database.updateMetaAndFirstLevelChildren(session, obj);
            obj.checkRename();
            map.remove(oldName);
            freeUniqueName(oldName);
            obj.rename(newName);
            map.put(newName, obj);
            freeUniqueName(newName);
        }
    }

    /**
     * Try to find a table or view with this name. This method returns null if
     * no object with this name exists. Local temporary tables are also returned.
     *
     * @param session the session
     * @param name the object name
     * @return the object or null
     */
    public Table findTableOrView(ServerSession session, String name) {
        Table table = tablesAndViews.get(name);
        if (table == null && session != null) {
            table = session.findLocalTempTable(name);
        }
        return table;
    }

    /**
     * Try to find an index with this name. This method returns null if
     * no object with this name exists.
     *
     * @param session the session
     * @param name the object name
     * @return the object or null
     */
    public Index findIndex(ServerSession session, String name) {
        Index index = indexes.get(name);
        if (index == null && session != null) {
            index = session.findLocalTempTableIndex(name);
        }
        return index;
    }

    /**
     * Try to find a trigger with this name. This method returns null if
     * no object with this name exists.
     *
     * @param name the object name
     * @return the object or null
     */
    public TriggerObject findTrigger(String name) {
        return triggers.get(name);
    }

    /**
     * Try to find a sequence with this name. This method returns null if
     * no object with this name exists.
     *
     * @param sequenceName the object name
     * @return the object or null
     */
    public Sequence findSequence(String sequenceName) {
        return sequences.get(sequenceName);
    }

    /**
     * Try to find a constraint with this name. This method returns null if no
     * object with this name exists.
     *
     * @param session the session
     * @param name the object name
     * @return the object or null
     */
    public Constraint findConstraint(ServerSession session, String name) {
        Constraint constraint = constraints.get(name);
        if (constraint == null && session != null) {
            constraint = session.findLocalTempTableConstraint(name);
        }
        return constraint;
    }

    /**
     * Try to find a user defined constant with this name. This method returns
     * null if no object with this name exists.
     *
     * @param constantName the object name
     * @return the object or null
     */
    public Constant findConstant(String constantName) {
        return constants.get(constantName);
    }

    /**
     * Try to find a user defined function with this name. This method returns
     * null if no object with this name exists.
     *
     * @param functionAlias the object name
     * @return the object or null
     */
    public FunctionAlias findFunction(String functionAlias) {
        return functions.get(functionAlias);
    }

    /**
     * Get the user defined aggregate function if it exists, or null if not.
     *
     * @param name the name of the user defined aggregate function
     * @return the aggregate function or null
     */
    public UserAggregate findAggregate(String name) {
        return aggregates.get(name);
    }

    /**
     * Get the user defined data type if it exists, or null if not.
     *
     * @param name the name of the user defined data type
     * @return the user defined data type or null
     */
    public UserDataType findUserDataType(String name) {
        return userDataTypes.get(name);
    }

    public Service findService(String serviceName) {
        return services.get(serviceName);
    }

    /**
     * Release a unique object name.
     *
     * @param name the object name
     */
    public void freeUniqueName(String name) {
        if (name != null) {
            synchronized (temporaryUniqueNames) {
                temporaryUniqueNames.remove(name);
            }
        }
    }

    private String getUniqueName(DbObject obj, HashMap<String, ? extends SchemaObject> map, String prefix) {
        String hash = Integer.toHexString(obj.getName().hashCode()).toUpperCase();
        String name = null;
        synchronized (temporaryUniqueNames) {
            for (int i = 1, len = hash.length(); i < len; i++) {
                name = prefix + hash.substring(0, i);
                if (!map.containsKey(name) && !temporaryUniqueNames.contains(name)) {
                    break;
                }
                name = null;
            }
            if (name == null) {
                prefix = prefix + hash + "_";
                for (int i = 0;; i++) {
                    name = prefix + i;
                    if (!map.containsKey(name) && !temporaryUniqueNames.contains(name)) {
                        break;
                    }
                }
            }
            temporaryUniqueNames.add(name);
        }
        return name;
    }

    /**
     * Create a unique constraint name.
     *
     * @param session the session
     * @param table the constraint table
     * @return the unique name
     */
    public String getUniqueConstraintName(ServerSession session, Table table) {
        HashMap<String, Constraint> tableConstraints;
        if (table.isTemporary() && !table.isGlobalTemporary()) {
            tableConstraints = session.getLocalTempTableConstraints();
        } else {
            tableConstraints = constraints;
        }
        return getUniqueName(table, tableConstraints, "CONSTRAINT_");
    }

    /**
     * Create a unique index name.
     *
     * @param session the session
     * @param table the indexed table
     * @param prefix the index name prefix
     * @return the unique name
     */
    public String getUniqueIndexName(ServerSession session, Table table, String prefix) {
        HashMap<String, Index> tableIndexes;
        if (table.isTemporary() && !table.isGlobalTemporary()) {
            tableIndexes = session.getLocalTempTableIndexes();
        } else {
            tableIndexes = indexes;
        }
        return getUniqueName(table, tableIndexes, prefix);
    }

    /**
     * Get the table or view with the given name.
     * Local temporary tables are also returned.
     *
     * @param session the session
     * @param name the table or view name
     * @return the table or view
     * @throws DbException if no such object exists
     */
    public Table getTableOrView(ServerSession session, String name) {
        Table table = tablesAndViews.get(name);
        if (table == null) {
            if (session != null) {
                table = session.findLocalTempTable(name);
            }
            if (table == null) {
                throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, name);
            }
        }
        return table;
    }

    /**
     * Get the index with the given name.
     *
     * @param name the index name
     * @return the index
     * @throws DbException if no such object exists
     */
    public Index getIndex(String name) {
        Index index = indexes.get(name);
        if (index == null) {
            throw DbException.get(ErrorCode.INDEX_NOT_FOUND_1, name);
        }
        return index;
    }

    /**
     * Get the constraint with the given name.
     *
     * @param name the constraint name
     * @return the constraint
     * @throws DbException if no such object exists
     */
    public Constraint getConstraint(String name) {
        Constraint constraint = constraints.get(name);
        if (constraint == null) {
            throw DbException.get(ErrorCode.CONSTRAINT_NOT_FOUND_1, name);
        }
        return constraint;
    }

    /**
     * Get the user defined constant with the given name.
     *
     * @param constantName the constant name
     * @return the constant
     * @throws DbException if no such object exists
     */
    public Constant getConstant(String constantName) {
        Constant constant = constants.get(constantName);
        if (constant == null) {
            throw DbException.get(ErrorCode.CONSTANT_NOT_FOUND_1, constantName);
        }
        return constant;
    }

    /**
     * Get the sequence with the given name.
     *
     * @param sequenceName the sequence name
     * @return the sequence
     * @throws DbException if no such object exists
     */
    public Sequence getSequence(String sequenceName) {
        Sequence sequence = sequences.get(sequenceName);
        if (sequence == null) {
            throw DbException.get(ErrorCode.SEQUENCE_NOT_FOUND_1, sequenceName);
        }
        return sequence;
    }

    /**
     * Get all objects.
     *
     * @return a (possible empty) list of all objects
     */
    public ArrayList<SchemaObject> getAll() {
        ArrayList<SchemaObject> all = new ArrayList<>();
        all.addAll(tablesAndViews.values());
        all.addAll(indexes.values());
        all.addAll(sequences.values());
        all.addAll(triggers.values());
        all.addAll(constraints.values());
        all.addAll(constants.values());
        all.addAll(functions.values());
        all.addAll(aggregates.values());
        all.addAll(userDataTypes.values());
        all.addAll(services.values());
        return all;
    }

    /**
     * Get all objects of the given type.
     *
     * @param type the object type
     * @return a (possible empty) list of all objects
     */
    public ArrayList<SchemaObject> getAll(DbObjectType type) {
        HashMap<String, SchemaObject> map = getMap(type);
        return new ArrayList<>(map.values());
    }

    /**
     * Get all tables and views.
     *
     * @return a (possible empty) list of all objects
     */
    public ArrayList<Table> getAllTablesAndViews() {
        return new ArrayList<>(tablesAndViews.values());
    }

    /**
     * Add a table to the schema.
     *
     * @param data the create table information
     * @return the created {@link Table} object
     */
    public Table createTable(CreateTableData data) {
        if (!data.temporary || data.globalTemporary) {
            database.lockMeta(data.session);
        }
        data.schema = this;

        if (data.isMemoryTable())
            data.storageEngineName = MemoryStorageEngine.NAME;

        // 用默认的数据库参数
        if (data.storageEngineName == null) {
            data.storageEngineName = database.getDefaultStorageEngineName();
        }
        if (data.storageEngineName != null) {
            StorageEngine engine = StorageEngineManager.getInstance().getEngine(data.storageEngineName);
            if (engine == null) {
                try {
                    engine = (StorageEngine) Utils.loadUserClass(data.storageEngineName).newInstance();
                    StorageEngineManager.getInstance().registerEngine(engine);
                } catch (Exception e) {
                    throw DbException.convert(e);
                }
            }

            if (engine instanceof TableFactory) {
                return ((TableFactory) engine).createTable(data);
            }
            return new StandardTable(data, engine);
        }
        throw DbException.convert(new NullPointerException("table engine is null"));
    }
}
