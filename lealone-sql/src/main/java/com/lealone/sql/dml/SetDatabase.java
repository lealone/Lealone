/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.dml;

import java.text.Collator;

import com.lealone.common.compress.CompressTool;
import com.lealone.common.compress.Compressor;
import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.MathUtils;
import com.lealone.common.util.Utils;
import com.lealone.db.Database;
import com.lealone.db.DbSetting;
import com.lealone.db.Mode;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Table;
import com.lealone.db.value.CompareMode;

/**
 * This class represents the statement
 * SET
 * 
 * @author H2 Group
 * @author zhh
 */
// 只处理database级的类型
public class SetDatabase extends SetStatement {

    private final DbSetting setting;
    private final Database database;
    private boolean changed;

    public SetDatabase(ServerSession session, DbSetting type) {
        super(session);
        this.setting = type;
        this.database = session.getDatabase();
    }

    @Override
    protected String getSettingName() {
        return setting.name();
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        // 需要加锁，不允许多个事务同时修改数据库的参数
        if (database.tryExclusiveDatabaseLock(session) == null)
            return -1;
        String name = setting.name();
        switch (setting) {
        case ALLOW_LITERALS: {
            int value = getIntValue();
            if (value < 0 || value > 2) {
                throw DbException.getInvalidValueException(name, value);
            }
            setDbSetting(value);
            break;
        }
        case CACHE_SIZE: {
            int value = getAndValidateIntValue();
            int max = MathUtils.convertLongToInt(Utils.getMemoryMax()) / 2;
            value = Math.min(value, max);
            setDbSetting(value);
            break;
        }
        case COLLATION: {
            Table table = database.getFirstUserTable();
            if (table != null) {
                throw DbException.get(ErrorCode.COLLATION_CHANGE_WITH_DATA_TABLE_1, table.getSQL());
            }
            final boolean binaryUnsigned = database.getCompareMode().isBinaryUnsigned();
            CompareMode compareMode;
            StringBuilder buff = new StringBuilder(stringValue);
            if (stringValue.equals(CompareMode.OFF)) {
                compareMode = CompareMode.getInstance(null, 0, binaryUnsigned);
            } else {
                int strength = getIntValue();
                buff.append(" STRENGTH ");
                if (strength == Collator.IDENTICAL) {
                    buff.append("IDENTICAL");
                } else if (strength == Collator.PRIMARY) {
                    buff.append("PRIMARY");
                } else if (strength == Collator.SECONDARY) {
                    buff.append("SECONDARY");
                } else if (strength == Collator.TERTIARY) {
                    buff.append("TERTIARY");
                }
                compareMode = CompareMode.getInstance(stringValue, strength, binaryUnsigned);
            }
            setDbSetting(buff.toString());
            database.setCompareMode(compareMode);
            break;
        }
        case BINARY_COLLATION: {
            Table table = database.getFirstUserTable();
            if (table != null) {
                throw DbException.get(ErrorCode.COLLATION_CHANGE_WITH_DATA_TABLE_1, table.getSQL());
            }
            CompareMode currentMode = database.getCompareMode();
            CompareMode newMode;
            if (stringValue.equals(CompareMode.SIGNED)) {
                newMode = CompareMode.getInstance(currentMode.getName(), currentMode.getStrength(),
                        false);
            } else if (stringValue.equals(CompareMode.UNSIGNED)) {
                newMode = CompareMode.getInstance(currentMode.getName(), currentMode.getStrength(),
                        true);
            } else {
                throw DbException.getInvalidValueException(name, stringValue);
            }
            setDbSetting(stringValue);
            database.setCompareMode(newMode);
            break;
        }
        case LOB_COMPRESSION_ALGORITHM: {
            int algo = CompressTool.getCompressAlgorithm(stringValue);
            setDbSetting(algo == Compressor.NO ? null : stringValue);
            break;
        }
        case DATABASE_EVENT_LISTENER: {
            // 先在setEventListenerClass中检查类是否存在，然后现构建DbSetting
            database.setEventListenerClass(stringValue);
            setDbSetting(stringValue);
            break;
        }
        case DB_CLOSE_DELAY: {
            int value = getIntValue();
            if (value == -1) {
                // -1 is a special value for in-memory databases,
                // which means "keep the DB alive and use the same DB for all connections"
            } else if (value < 0) {
                throw DbException.getInvalidValueException(name, value);
            }
            setDbSetting(value);
            database.setCloseDelay(value);
            break;
        }
        case DEFAULT_LOCK_TIMEOUT: {
            setDbSetting(getAndValidateIntValue());
            break;
        }
        case DEFAULT_TABLE_TYPE: {
            int value = getIntValue();
            if (value < 0 || value > 1) {
                throw DbException.getInvalidValueException(name, value);
            }
            setDbSetting(value);
            break;
        }
        case EXCLUSIVE: {
            int value = getIntValue();
            switch (value) {
            case 0:
                database.setExclusiveSession(null, false);
                break;
            case 1:
                database.setExclusiveSession(session, false);
                break;
            case 2:
                database.setExclusiveSession(session, true);
                break;
            default:
                throw DbException.getInvalidValueException(name, value);
            }
            break;
        }
        case IGNORECASE: {
            int value = getIntValue();
            setDbSetting(value == 1);
            break;
        }
        case MAX_LENGTH_INPLACE_LOB: {
            int value = getAndValidateIntValue();
            setDbSetting(value);
            break;
        }
        case MAX_MEMORY_ROWS: {
            int value = getAndValidateIntValue();
            setDbSetting(value);
            break;
        }
        case MAX_MEMORY_UNDO: {
            int value = getAndValidateIntValue();
            setDbSetting(value);
            break;
        }
        case MAX_OPERATION_MEMORY: {
            int value = getAndValidateIntValue();
            setDbSetting(value);
            break;
        }
        case MODE: {
            String m = getStringValue();
            Mode mode = Mode.getInstance(m);
            if (mode == null) {
                throw DbException.get(ErrorCode.UNKNOWN_MODE_1, m);
            }
            if (database.getMode() != mode) {
                database.setMode(mode);
            }
            setDbSetting(m);
            break;
        }
        case READ_ONLY: {
            boolean value = getAndValidateBooleanValue();
            setDbSetting(value);
            database.setReadOnly(value);
            break;
        }
        case OPTIMIZE_REUSE_RESULTS: {
            boolean value = getAndValidateBooleanValue();
            setDbSetting(value);
            break;
        }
        case REFERENTIAL_INTEGRITY: {
            boolean value = getAndValidateBooleanValue();
            setDbSetting(value);
            break;
        }
        case QUERY_STATISTICS: {
            boolean value = getAndValidateBooleanValue();
            setDbSetting(value);
            database.setQueryStatistics(value);
            break;
        }
        case QUERY_STATISTICS_MAX_ENTRIES: {
            int value = getAndValidateIntValue(1);
            setDbSetting(value);
            database.setQueryStatisticsMaxEntries(value);
            break;
        }
        case TRACE_LEVEL_SYSTEM_OUT: {
            if (getCurrentObjectId() == 0) {
                // don't set the property when opening the database
                // this is for compatibility with older versions, because
                // this setting was persistent
                int value = getIntValue();
                setDbSetting(value);
                database.getTraceSystem().setLevelSystemOut(value);
            }
            break;
        }
        case TRACE_LEVEL_FILE: {
            if (getCurrentObjectId() == 0) {
                // don't set the property when opening the database
                // this is for compatibility with older versions, because
                // this setting was persistent
                int value = getIntValue();
                setDbSetting(value);
                database.getTraceSystem().setLevelFile(value);
            }
            break;
        }
        case TRACE_MAX_FILE_SIZE: {
            int value = getAndValidateIntValue();
            int size = value * 1024 * 1024;
            database.getTraceSystem().setMaxFileSize(size);
            setDbSetting(value);
            break;
        }
        case QUERY_CACHE_SIZE: {
            int value = getAndValidateIntValue();
            setDbSetting(value);
            for (ServerSession s : database.getSessions(false)) {
                s.setQueryCacheSize(value);
            }
            break;
        }
        default:
            if (DbSetting.contains(name)) {
                setDbSetting(getStringValue());
            } else {
                DbException.throwInternalError("unknown setting type: " + setting);
            }
        }
        if (changed)
            databaseChanged(database);
        return 0;
    }

    private void setDbSetting(int v) {
        setDbSetting(String.valueOf(v));
    }

    private void setDbSetting(boolean v) {
        setDbSetting(String.valueOf(v));
    }

    private void setDbSetting(String v) {
        changed = database.setDbSetting(session, setting, v);
    }
}
