/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import java.util.HashMap;
import java.util.Map;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.TraceSystem;
import org.lealone.common.util.StringUtils;
import org.lealone.common.util.Utils;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.table.Table;

/**
 * This class contains various database-level settings. To override the
 * documented default value for a database, append the setting in the database
 * URL: "jdbc:lealone:test;ALIAS_COLUMN_NAME=TRUE" when opening the first connection
 * to the database. The settings can not be changed once the database is open.
 * <p>
 * Some settings are a last resort and temporary solution to work around a
 * problem in the application or database engine. Also, there are system
 * properties to enable features that are not yet fully tested or that are not
 * backward compatible.
 * </p>
 */
// 可以通过系统属性的方式来设定每个数据库的默认值，比如lealone.x.y.z = 10，
// 还可以通过在URL中指定参数来覆盖默认值，在URL中的参数格式是x_y_z = 10(没有lealone前缀，用下划线替换点号)
class SettingsBase {
    // 这个字段需要放在这，在DbSettings的构造函数中可以提前初始化它，
    // 如果放在子类中，DbSettings类的其他字段会在执行构造函数中的代码之前执行，此时settings还是null。
    protected final Map<String, String> settings;

    protected SettingsBase(Map<String, String> s) {
        this.settings = new HashMap<>(s);
    }
}

public class DbSettings extends SettingsBase {

    private static final DbSettings defaultSettings = new DbSettings(new HashMap<String, String>());

    public static DbSettings getDefaultSettings() {
        return defaultSettings;
    }

    /**
     * INTERNAL.
     * Get the settings for the given properties (may be null).
     *
     * @param s the settings
     * @return the settings
     */
    public static DbSettings getInstance(Map<String, String> s) {
        if (s == null || s.isEmpty()) {
            return defaultSettings;
        }
        return new DbSettings(s);
    }

    private DbSettings(Map<String, String> s) {
        super(s);
    }

    /**
     * Database setting <code>ALIAS_COLUMN_NAME</code> (default: false).<br />
     * When enabled, aliased columns (as in SELECT ID AS I FROM TEST) return the
     * alias (I in this case) in ResultSetMetaData.getColumnName() and 'null' in
     * getTableName(). If disabled, the real column name (ID in this case) and
     * table name is returned.
     * <br />
     * This setting only affects the default and the MySQL mode. When using
     * any other mode, this feature is enabled for compatibility, even if this
     * database setting is not enabled explicitly.
     */
    public final boolean aliasColumnName = get(DbSetting.ALIAS_COLUMN_NAME, false);

    /**
     * Database setting <code>ANALYZE_AUTO</code> (default: 2000).<br />
     * After changing this many rows, ANALYZE is automatically run for a table.
     * Automatically running ANALYZE is disabled if set to 0. If set to 1000,
     * then ANALYZE will run against each user table after about 1000 changes to
     * that table. The time between running ANALYZE doubles each time since
     * starting the database. It is not run on local temporary tables, and
     * tables that have a trigger on SELECT.
     */
    public final int analyzeAuto = get(DbSetting.ANALYZE_AUTO, 2000);

    /**
     * Database setting <code>ANALYZE_SAMPLE</code> (default: 10000).<br />
     * The default sample size when analyzing a table.
     */
    public final int analyzeSample = get(DbSetting.ANALYZE_SAMPLE, 10000);

    /**
     * Database setting <code>DATABASE_TO_UPPER</code> (default: true).<br />
     * Database short names are converted to uppercase for the DATABASE()
     * function, and in the CATALOG column of all database meta data methods.
     * Setting this to "false" is experimental. When set to false, all
     * identifier names (table names, column names) are case sensitive (except
     * aggregate, built-in functions, data types, and keywords).
     */
    public final boolean databaseToUpper = get(DbSetting.DATABASE_TO_UPPER, true);

    /**
     * Database setting <code>DB_CLOSE_ON_EXIT</code> (default: true).<br />
     * Close the database when the virtual machine exits normally, using a
     * shutdown hook.
     */
    public final boolean dbCloseOnExit = get(DbSetting.DB_CLOSE_ON_EXIT, true);

    /**
     * Database setting <code>DEFAULT_ESCAPE</code> (default: \).<br />
     * The default escape character for LIKE comparisons. To select no escape
     * character, use an empty string.
     */
    public final String defaultEscape = get(DbSetting.DEFAULT_ESCAPE, "\\");

    /**
     * Database setting <code>DROP_RESTRICT</code> (default: true).<br />
     * Whether the default action for DROP TABLE and DROP VIEW is RESTRICT.
     */
    public final boolean dropRestrict = get(DbSetting.DROP_RESTRICT, true);

    /**
     * Database setting <code>EARLY_FILTER</code> (default: false).<br />
     * This setting allows table implementations to apply filter conditions
     * early on.
     */
    public final boolean earlyFilter = get(DbSetting.EARLY_FILTER, false);

    /**
     * Database setting <code>ESTIMATED_FUNCTION_TABLE_ROWS</code> (default:
     * 1000).<br />
     * The estimated number of rows in a function table (for example, CSVREAD or
     * FTL_SEARCH). This value is used by the optimizer.
     */
    public final int estimatedFunctionTableRows = get(DbSetting.ESTIMATED_FUNCTION_TABLE_ROWS, 1000);

    /**
     * Database setting <code>FUNCTIONS_IN_SCHEMA</code> (default:
     * true).<br />
     * If set, all functions are stored in a schema. Specially, the SCRIPT statement
     * will always include the schema name in the CREATE ALIAS statement.
     * This is not backward compatible with H2 versions 1.2.134 and older.
     */
    public final boolean functionsInSchema = get(DbSetting.FUNCTIONS_IN_SCHEMA, true);

    /**
     * Database setting <code>LARGE_RESULT_BUFFER_SIZE</code> (default: 4096).<br />
     * Buffer size for large result sets. Set this value to 0 to disable the buffer.
     */
    public final int largeResultBufferSize = get(DbSetting.LARGE_RESULT_BUFFER_SIZE, 4 * 1024);

    /**
     * Database setting <code>LARGE_TRANSACTIONS</code> (default: true).<br />
     * Support very large transactions
     */
    public final boolean largeTransactions = get(DbSetting.LARGE_TRANSACTIONS, true); // TODO 是否考虑用在UndoLog中

    /**
     * Database setting <code>MAX_COMPACT_TIME</code> (default: 200).<br />
     * The maximum time in milliseconds used to compact a database when closing.
     */
    public final int maxCompactTime = get(DbSetting.MAX_COMPACT_TIME, 200); // TODO 这个参数可以用到存储引擎中

    /**
     * Database setting <code>MAX_MEMORY_ROWS_DISTINCT</code> (default:
     * 10000).<br />
     * The maximum number of rows kept in-memory for SELECT DISTINCT queries. If
     * more than this number of rows are in a result set, a temporary table is used.
     */
    public final int maxMemoryRowsDistinct = get(DbSetting.MAX_MEMORY_ROWS_DISTINCT, 10000);

    /**
     * Database setting <code>MAX_QUERY_TIMEOUT</code> (default: 0).<br />
     * The maximum timeout of a query in milliseconds. The default is 0, meaning
     * no limit. Please note the actual query timeout may be set to a lower value.
     */
    public final int maxQueryTimeout = get(DbSetting.MAX_QUERY_TIMEOUT, 0);

    /**
     * Database setting <code>NESTED_JOINS</code> (default: true).<br />
     * Whether nested joins should be supported.
     */
    public final boolean nestedJoins = get(DbSetting.NESTED_JOINS, true);

    /**
     * Database setting <code>OPTIMIZE_DISTINCT</code> (default: true).<br />
     * Improve the performance of simple DISTINCT queries if an index is
     * available for the given column. The optimization is used if:
     * <ul>
     * <li>The select is a single column query without condition </li>
     * <li>The query contains only one table, and no group by </li>
     * <li>There is only one table involved </li>
     * <li>There is an ascending index on the column </li>
     * <li>The selectivity of the column is below 20 </li>
     * </ul>
     */
    public final boolean optimizeDistinct = get(DbSetting.OPTIMIZE_DISTINCT, true);

    /**
     * Database setting <code>OPTIMIZE_EVALUATABLE_SUBQUERIES</code> (default: true).<br />
     * Optimize subqueries that are not dependent on the outer query.
     */
    public final boolean optimizeEvaluatableSubqueries = get(DbSetting.OPTIMIZE_EVALUATABLE_SUBQUERIES, true);

    /**
     * Database setting <code>OPTIMIZE_INSERT_FROM_SELECT</code>
     * (default: true).<br />
     * Insert into table from query directly bypassing temporary disk storage.
     * This also applies to create table as select.
     */
    public final boolean optimizeInsertFromSelect = get(DbSetting.OPTIMIZE_INSERT_FROM_SELECT, true);

    /**
     * Database setting <code>OPTIMIZE_IN_LIST</code> (default: true).<br />
     * Optimize IN(...) and IN(SELECT ...) comparisons. This includes
     * optimization for SELECT, DELETE, and UPDATE.
     */
    public final boolean optimizeInList = get(DbSetting.OPTIMIZE_IN_LIST, true);

    /**
     * Database setting <code>OPTIMIZE_IN_SELECT</code> (default: true).<br />
     * Optimize IN(SELECT ...) comparisons. This includes
     * optimization for SELECT, DELETE, and UPDATE.
     */
    public final boolean optimizeInSelect = get(DbSetting.OPTIMIZE_IN_SELECT, true);

    /**
     * Database setting <code>OPTIMIZE_IS_NULL</code> (default: true).<br />
     * Use an index for condition of the form columnName IS NULL.
     */
    public final boolean optimizeIsNull = get(DbSetting.OPTIMIZE_IS_NULL, true);

    /**
     * Database setting <code>OPTIMIZE_OR</code> (default: true).<br />
     * Convert (C=? OR C=?) to (C IN(?, ?)).
     */
    public final boolean optimizeOr = get(DbSetting.OPTIMIZE_OR, true);

    /**
     * Database setting <code>OPTIMIZE_TWO_EQUALS</code> (default: true).<br />
     * Optimize expressions of the form A=B AND B=1. In this case, AND A=1 is
     * added so an index on A can be used.
     */
    public final boolean optimizeTwoEquals = get(DbSetting.OPTIMIZE_TWO_EQUALS, true);

    /**
     * Database setting <code>QUERY_CACHE_SIZE</code> (default: 8).<br />
     * The size of the query cache, in number of cached statements. Each session
     * has it's own cache with the given size. The cache is only used if the SQL
     * statement and all parameters match. Only the last returned result per
     * query is cached. Only SELECT statements are cached (excluding UNION and
     * FOR UPDATE statements). This works for both statements and prepared
     * statement.
     */
    public final int queryCacheSize = get(DbSetting.QUERY_CACHE_SIZE, 8);

    /**
     * Database setting <code>RECOMPILE_ALWAYS</code> (default: false).<br />
     * Always recompile prepared statements.
     */
    public final boolean recompileAlways = get(DbSetting.RECOMPILE_ALWAYS, false);

    /**
     * Database setting <code>ROWID</code> (default: true).<br />
     * If set, each table has a pseudo-column _ROWID_.
     */
    public final boolean rowId = get(DbSetting.ROWID, true);

    /**
     * Database setting <code>SELECT_FOR_UPDATE_MVCC</code>
     * (default: true).<br />
     * If set, SELECT .. FOR UPDATE queries lock only the selected rows when
     * using MVCC.
     */
    public final boolean selectForUpdateMvcc = get(DbSetting.SELECT_FOR_UPDATE_MVCC, true);

    /**
     * Database setting <code>DEFAULT_STORAGE_ENGINE</code>
     * (default: AOSE).<br />
     * The default storage engine to use for new tables.
     */
    public final String defaultStorageEngine = get(DbSetting.DEFAULT_STORAGE_ENGINE,
            Constants.DEFAULT_STORAGE_ENGINE_NAME);

    /**
     * Database setting <code>DEFAULT_SQL_ENGINE</code>
     * (default: lealone).<br />
     * The default sql engine.
     */
    public final String defaultSQLEngine = get(DbSetting.DEFAULT_SQL_ENGINE, Constants.DEFAULT_SQL_ENGINE_NAME);

    /**
     * Database setting <code>DEFAULT_TRANSACTION_ENGINE</code>
     * (default: AOTE).<br />
     * The default transaction engine.
     */
    public final String defaultTransactionEngine = get(DbSetting.DEFAULT_TRANSACTION_ENGINE,
            Constants.DEFAULT_TRANSACTION_ENGINE_NAME);

    /**
     * Database setting <code>COMPRESS</code>
     * (default: false).<br />
     * Compress data when storing.
     */
    public final boolean compressData = get(DbSetting.COMPRESS, false);

    /**
     * Database setting <code>PERSISTENT</code>
     * (default: true).<br />
     * Persistent data.
     */
    public final boolean persistent = get(DbSetting.PERSISTENT, true);

    public final int cacheSize = get(DbSetting.CACHE_SIZE, Constants.DEFAULT_CACHE_SIZE);
    public final int pageSize = get(DbSetting.PAGE_SIZE, Constants.DEFAULT_PAGE_SIZE);
    public final String eventListener = get(DbSetting.DATABASE_EVENT_LISTENER, null);
    public final String mode = get(DbSetting.MODE, null);
    public final String cipher = get(DbSetting.CIPHER, null);
    public final byte[] filePasswordHash = convertHexToBytes(DbSetting.FILE_PASSWORD_HASH, null);
    public final byte[] fileEncryptionKey = convertHexToBytes(DbSetting.FILE_ENCRYPTION_KEY, null);
    public final int traceLevelFile = get(DbSetting.TRACE_LEVEL_FILE, TraceSystem.DEFAULT_TRACE_LEVEL_FILE);
    public final int traceLevelSystemOut = get(DbSetting.TRACE_LEVEL_SYSTEM_OUT,
            TraceSystem.DEFAULT_TRACE_LEVEL_SYSTEM_OUT);

    public final boolean readOnly = get(DbSetting.READ_ONLY, false);

    public final int allowLiterals = get(DbSetting.ALLOW_LITERALS, Constants.ALLOW_LITERALS_ALL);
    public final String collation = get(DbSetting.COLLATION, null);
    public final String binaryCollation = get(DbSetting.BINARY_COLLATION, null);
    public final String lobCompressionAlgorithm = get(DbSetting.LOB_COMPRESSION_ALGORITHM, null);
    public final int dbCloseDelay = get(DbSetting.DB_CLOSE_DELAY, -1);
    public final int defaultLockTimeout = get(DbSetting.DEFAULT_LOCK_TIMEOUT, Constants.INITIAL_LOCK_TIMEOUT);
    public final int defaultTableType = get(DbSetting.DEFAULT_TABLE_TYPE, Table.TYPE_CACHED);
    public final boolean ignoreCase = get(DbSetting.IGNORECASE, false);
    public final int lockMode = get(DbSetting.LOCK_MODE, Constants.DEFAULT_LOCK_MODE);

    public final int maxLengthInplaceLob = get(DbSetting.MAX_LENGTH_INPLACE_LOB,
            SysProperties.LOB_IN_DATABASE ? Constants.DEFAULT_MAX_LENGTH_INPLACE_LOB2
                    : Constants.DEFAULT_MAX_LENGTH_INPLACE_LOB);

    public final int maxMemoryRows = get(DbSetting.MAX_MEMORY_ROWS, Constants.DEFAULT_MAX_MEMORY_ROWS);
    public final int maxMemoryUndo = get(DbSetting.MAX_MEMORY_UNDO, Constants.DEFAULT_MAX_MEMORY_UNDO);
    public final int maxOperationMemory = get(DbSetting.MAX_OPERATION_MEMORY, Constants.DEFAULT_MAX_OPERATION_MEMORY);

    public final boolean optimizeReuseResults = get(DbSetting.OPTIMIZE_REUSE_RESULTS, true);
    public final boolean referentialIntegrity = get(DbSetting.REFERENTIAL_INTEGRITY, true);

    public final boolean queryStatistics = get(DbSetting.QUERY_STATISTICS, false);
    public final int queryStatisticsMaxEntries = get(DbSetting.QUERY_STATISTICS_MAX_ENTRIES,
            Constants.QUERY_STATISTICS_MAX_ENTRIES);

    public final int writeDelay = get(DbSetting.WRITE_DELAY, Constants.DEFAULT_WRITE_DELAY);

    /**
     * Get the setting for the given key.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the setting
     */
    private boolean get(DbSetting key, boolean defaultValue) {
        String s = get(key, defaultValue ? "true" : "false");
        try {
            return Boolean.parseBoolean(s);
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, "key:" + key + " value:" + s);
        }
    }

    /**
     * Get the setting for the given key.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the setting
     */
    private int get(DbSetting key, int defaultValue) {
        String s = get(key, Integer.toString(defaultValue));
        try {
            return Integer.decode(s);
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, "key:" + key + " value:" + s);
        }
    }

    /**
     * Get the setting for the given key.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the setting
     */
    private String get(DbSetting key, String defaultValue) {
        String name = key.getName();
        StringBuilder buff = new StringBuilder(Constants.PROJECT_NAME_PREFIX);
        for (char c : name.toCharArray()) {
            if (c == '_') {
                buff.append('.');
            } else {
                // Character.toUpperCase / toLowerCase ignores the locale
                buff.append(Character.toLowerCase(c));
            }
        }
        String sysProperty = buff.toString();
        String v = settings.get(name);
        if (v == null) {
            v = Utils.getProperty(sysProperty, defaultValue);
            settings.put(name, v);
        }
        return v;
    }

    private byte[] convertHexToBytes(DbSetting key, String defaultValue) {
        String v = get(key, defaultValue);
        return v == null ? null : StringUtils.convertHexToBytes(v);
    }

    /**
     * Get all settings.
     *
     * @return the settings
     */
    public Map<String, String> getSettings() {
        return settings;
    }
}
