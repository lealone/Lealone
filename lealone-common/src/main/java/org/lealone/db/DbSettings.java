/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import java.util.HashMap;
import java.util.Map;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.TraceSystem;
import org.lealone.common.util.StringUtils;
import org.lealone.common.util.Utils;

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
    public final boolean aliasColumnName = get("ALIAS_COLUMN_NAME", false);

    /**
     * Database setting <code>ANALYZE_AUTO</code> (default: 2000).<br />
     * After changing this many rows, ANALYZE is automatically run for a table.
     * Automatically running ANALYZE is disabled if set to 0. If set to 1000,
     * then ANALYZE will run against each user table after about 1000 changes to
     * that table. The time between running ANALYZE doubles each time since
     * starting the database. It is not run on local temporary tables, and
     * tables that have a trigger on SELECT.
     */
    public final int analyzeAuto = get("ANALYZE_AUTO", 2000);

    /**
     * Database setting <code>ANALYZE_SAMPLE</code> (default: 10000).<br />
     * The default sample size when analyzing a table.
     */
    public final int analyzeSample = get("ANALYZE_SAMPLE", 10000);

    /**
     * Database setting <code>DATABASE_TO_UPPER</code> (default: true).<br />
     * Database short names are converted to uppercase for the DATABASE()
     * function, and in the CATALOG column of all database meta data methods.
     * Setting this to "false" is experimental. When set to false, all
     * identifier names (table names, column names) are case sensitive (except
     * aggregate, built-in functions, data types, and keywords).
     */
    public final boolean databaseToUpper = get("DATABASE_TO_UPPER", true);

    /**
     * Database setting <code>DB_CLOSE_ON_EXIT</code> (default: true).<br />
     * Close the database when the virtual machine exits normally, using a
     * shutdown hook.
     */
    public final boolean dbCloseOnExit = get("DB_CLOSE_ON_EXIT", true);

    /**
     * Database setting <code>DEFAULT_CONNECTION</code> (default: false).<br />
     * Whether Java functions can use
     * <code>DriverManager.getConnection("jdbc:default:connection")</code> to
     * get a database connection. This feature is disabled by default for
     * performance reasons. Please note the Oracle JDBC driver will try to
     * resolve this database URL if it is loaded before the H2 driver.
     */
    public boolean defaultConnection = get("DEFAULT_CONNECTION", false);

    /**
     * Database setting <code>DEFAULT_ESCAPE</code> (default: \).<br />
     * The default escape character for LIKE comparisons. To select no escape
     * character, use an empty string.
     */
    public final String defaultEscape = get("DEFAULT_ESCAPE", "\\");

    /**
     * Database setting <code>DEFRAG_ALWAYS</code> (default: false).<br />
     * Each time the database is closed, it is fully defragmented (SHUTDOWN DEFRAG).
     */
    public final boolean defragAlways = get("DEFRAG_ALWAYS", false); // TODO 这个参数可以用到存储引擎中

    /**
     * Database setting <code>DROP_RESTRICT</code> (default: true).<br />
     * Whether the default action for DROP TABLE and DROP VIEW is RESTRICT.
     */
    public final boolean dropRestrict = get("DROP_RESTRICT", true);

    /**
     * Database setting <code>EARLY_FILTER</code> (default: false).<br />
     * This setting allows table implementations to apply filter conditions
     * early on.
     */
    public final boolean earlyFilter = get("EARLY_FILTER", false);

    /**
     * Database setting <code>ESTIMATED_FUNCTION_TABLE_ROWS</code> (default:
     * 1000).<br />
     * The estimated number of rows in a function table (for example, CSVREAD or
     * FTL_SEARCH). This value is used by the optimizer.
     */
    public final int estimatedFunctionTableRows = get("ESTIMATED_FUNCTION_TABLE_ROWS", 1000);

    /**
     * Database setting <code>FUNCTIONS_IN_SCHEMA</code> (default:
     * true).<br />
     * If set, all functions are stored in a schema. Specially, the SCRIPT statement
     * will always include the schema name in the CREATE ALIAS statement.
     * This is not backward compatible with H2 versions 1.2.134 and older.
     */
    public final boolean functionsInSchema = get("FUNCTIONS_IN_SCHEMA", true);

    /**
     * Database setting <code>LARGE_RESULT_BUFFER_SIZE</code> (default: 4096).<br />
     * Buffer size for large result sets. Set this value to 0 to disable the
     * buffer.
     */
    public final int largeResultBufferSize = get("LARGE_RESULT_BUFFER_SIZE", 4 * 1024);

    /**
     * Database setting <code>LARGE_TRANSACTIONS</code> (default: true).<br />
     * Support very large transactions
     */
    public final boolean largeTransactions = get("LARGE_TRANSACTIONS", true); // TODO 是否考虑用在UndoLog中

    /**
     * Database setting <code>MAX_COMPACT_TIME</code> (default: 200).<br />
     * The maximum time in milliseconds used to compact a database when closing.
     */
    public final int maxCompactTime = get("MAX_COMPACT_TIME", 200); // TODO 这个参数可以用到存储引擎中

    /**
     * Database setting <code>MAX_MEMORY_ROWS_DISTINCT</code> (default:
     * 10000).<br />
     * The maximum number of rows kept in-memory for SELECT DISTINCT queries. If
     * more than this number of rows are in a result set, a temporary table is
     * used.
     */
    public final int maxMemoryRowsDistinct = get("MAX_MEMORY_ROWS_DISTINCT", 10000);

    /**
     * Database setting <code>MAX_QUERY_TIMEOUT</code> (default: 0).<br />
     * The maximum timeout of a query in milliseconds. The default is 0, meaning
     * no limit. Please note the actual query timeout may be set to a lower
     * value.
     */
    public int maxQueryTimeout = get("MAX_QUERY_TIMEOUT", 0);

    /**
     * Database setting <code>NESTED_JOINS</code> (default: true).<br />
     * Whether nested joins should be supported.
     */
    public final boolean nestedJoins = get("NESTED_JOINS", true);

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
    public final boolean optimizeDistinct = get("OPTIMIZE_DISTINCT", true);

    /**
     * Database setting <code>OPTIMIZE_EVALUATABLE_SUBQUERIES</code> (default:
     * true).<br />
     * Optimize subqueries that are not dependent on the outer query.
     */
    public final boolean optimizeEvaluatableSubqueries = get("OPTIMIZE_EVALUATABLE_SUBQUERIES", true);

    /**
     * Database setting <code>OPTIMIZE_INSERT_FROM_SELECT</code>
     * (default: true).<br />
     * Insert into table from query directly bypassing temporary disk storage.
     * This also applies to create table as select.
     */
    public final boolean optimizeInsertFromSelect = get("OPTIMIZE_INSERT_FROM_SELECT", true);

    /**
     * Database setting <code>OPTIMIZE_IN_LIST</code> (default: true).<br />
     * Optimize IN(...) and IN(SELECT ...) comparisons. This includes
     * optimization for SELECT, DELETE, and UPDATE.
     */
    public final boolean optimizeInList = get("OPTIMIZE_IN_LIST", true);

    /**
     * Database setting <code>OPTIMIZE_IN_SELECT</code> (default: true).<br />
     * Optimize IN(SELECT ...) comparisons. This includes
     * optimization for SELECT, DELETE, and UPDATE.
     */
    public final boolean optimizeInSelect = get("OPTIMIZE_IN_SELECT", true);

    /**
     * Database setting <code>OPTIMIZE_IS_NULL</code> (default: false).<br />
     * Use an index for condition of the form columnName IS NULL.
     */
    public final boolean optimizeIsNull = get("OPTIMIZE_IS_NULL", true);

    /**
     * Database setting <code>OPTIMIZE_OR</code> (default: true).<br />
     * Convert (C=? OR C=?) to (C IN(?, ?)).
     */
    public final boolean optimizeOr = get("OPTIMIZE_OR", true);

    /**
     * Database setting <code>OPTIMIZE_TWO_EQUALS</code> (default: true).<br />
     * Optimize expressions of the form A=B AND B=1. In this case, AND A=1 is
     * added so an index on A can be used.
     */
    public final boolean optimizeTwoEquals = get("OPTIMIZE_TWO_EQUALS", true);

    /**
     * Database setting <code>QUERY_CACHE_SIZE</code> (default: 8).<br />
     * The size of the query cache, in number of cached statements. Each session
     * has it's own cache with the given size. The cache is only used if the SQL
     * statement and all parameters match. Only the last returned result per
     * query is cached. Only SELECT statements are cached (excluding UNION and
     * FOR UPDATE statements). This works for both statements and prepared
     * statement.
     */
    public final int queryCacheSize = get("QUERY_CACHE_SIZE", 8);

    /**
     * Database setting <code>RECOMPILE_ALWAYS</code> (default: false).<br />
     * Always recompile prepared statements.
     */
    public final boolean recompileAlways = get("RECOMPILE_ALWAYS", false);

    /**
     * Database setting <code>ROWID</code> (default: true).<br />
     * If set, each table has a pseudo-column _ROWID_.
     */
    public final boolean rowId = get("ROWID", true);

    /**
     * Database setting <code>SELECT_FOR_UPDATE_MVCC</code>
     * (default: true).<br />
     * If set, SELECT .. FOR UPDATE queries lock only the selected rows when
     * using MVCC.
     */
    public final boolean selectForUpdateMvcc = get("SELECT_FOR_UPDATE_MVCC", true);

    /**
     * Database setting <code>DEFAULT_STORAGE_ENGINE</code>
     * (default: AOSE).<br />
     * The default storage engine to use for new tables.
     */
    public final String defaultStorageEngine = get("DEFAULT_STORAGE_ENGINE", Constants.DEFAULT_STORAGE_ENGINE_NAME);

    /**
     * Database setting <code>DEFAULT_SQL_ENGINE</code>
     * (default: lealone).<br />
     * The default sql engine.
     */
    public final String defaultSQLEngine = get("DEFAULT_SQL_ENGINE", Constants.DEFAULT_SQL_ENGINE_NAME);

    /**
     * Database setting <code>DEFAULT_TRANSACTION_ENGINE</code>
     * (default: MVCC).<br />
     * The default transaction engine.
     */
    public final String defaultTransactionEngine = get("DEFAULT_TRANSACTION_ENGINE",
            Constants.DEFAULT_TRANSACTION_ENGINE_NAME);

    /**
     * Database setting <code>DEFAULT_CONTAINER_ENGINE</code>
     * (default: CGROUP).<br />
     * The default container engine.
     */
    public final String defaultContainerEngine = get("DEFAULT_CONTAINER_ENGINE",
            Constants.DEFAULT_CONTAINER_ENGINE_NAME);

    /**
     * Database setting <code>COMPRESS</code>
     * (default: false).<br />
     * Compress data when storing.
     */
    public final boolean compressData = get("COMPRESS", false);

    /**
     * Database setting <code>PERSISTENT</code>
     * (default: true).<br />
     * Persistent data.
     */
    public final boolean persistent = get("PERSISTENT", true);

    public final int cpu = get("CPU", 0);

    public final int memory = get("MEMORY", 0);

    public final int net = get("NET", 0);

    public final int blockIo = get("BLOCK_IO", 0);

    public final int serviceLevel = get("SERVICE_LEVEL", 0);

    public final int cacheSize = get("CACHE_SIZE", Constants.DEFAULT_CACHE_SIZE);
    public final int pageSize = get("PAGE_SIZE", Constants.DEFAULT_PAGE_SIZE);
    public final String eventListener = get("DATABASE_EVENT_LISTENER", null);
    public final String mode = get("MODE", null);
    public final String cipher = get("CIPHER", null);
    public final byte[] filePasswordHash = convertHexToBytes("FILE_PASSWORD_HASH", null);
    public final byte[] fileEncryptionKey = convertHexToBytes("FILE_ENCRYPTION_KEY", null);
    public final int traceLevelFile = get("TRACE_LEVEL_FILE", TraceSystem.DEFAULT_TRACE_LEVEL_FILE);
    public final int traceLevelSystemOut = get("TRACE_LEVEL_SYSTEM_OUT", TraceSystem.DEFAULT_TRACE_LEVEL_SYSTEM_OUT);

    /**
     * Get the setting for the given key.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the setting
     */
    private boolean get(String key, boolean defaultValue) {
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
    private int get(String key, int defaultValue) {
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
    protected String get(String key, String defaultValue) {
        StringBuilder buff = new StringBuilder(Constants.PROJECT_NAME_PREFIX);
        for (char c : key.toCharArray()) {
            if (c == '_') {
                buff.append('.');
            } else {
                // Character.toUpperCase / toLowerCase ignores the locale
                buff.append(Character.toLowerCase(c));
            }
        }
        String sysProperty = buff.toString();
        String v = settings.get(key);
        if (v == null) {
            v = Utils.getProperty(sysProperty, defaultValue);
            settings.put(key, v);
        }
        return v;
    }

    private byte[] convertHexToBytes(String key, String defaultValue) {
        String v = get(key, defaultValue);
        return v == null ? null : StringUtils.convertHexToBytes(v);
    }

    /**
     * Check if the settings contains the given key.
     *
     * @param k the key
     * @return true if they do
     */
    public boolean containsKey(String k) {
        return settings.containsKey(k);
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
