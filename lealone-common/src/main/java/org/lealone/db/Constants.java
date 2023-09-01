/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import java.io.File;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.util.Properties;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.Utils;

/**
 * Constants are fixed values that are used in the whole database code.
 * 
 * @author H2 Group
 * @author zhh
 */
public class Constants {
    /**
     * 项目名称.
     */
    public static final String PROJECT_NAME = "lealone";

    /**
     * 项目名称前缀.
     */
    public static final String PROJECT_NAME_PREFIX = PROJECT_NAME + ".";

    public static final String DEFAULT_BASE_DIR = "." + File.separator + "target" + File.separator
            + "lealone_db_base_dir";

    public static final String DEFAULT_STORAGE_ENGINE_NAME = "AOSE";

    public static final String DEFAULT_TRANSACTION_ENGINE_NAME = "AOTE";

    public static final String DEFAULT_SQL_ENGINE_NAME = PROJECT_NAME;

    public static final int DEFAULT_NETWORK_TIMEOUT = 15000; // 默认15秒无响应就超时

    public static final String DEFAULT_NET_FACTORY_NAME = "nio";

    public static final String NET_FACTORY_NAME_KEY = ConnectionSetting.NET_FACTORY_NAME.name();

    public static final char NAME_SEPARATOR = '_';

    public static final String RESOURCES_DIR = "/org/lealone/common/resources/";

    /**
     * The TCP protocol version number 1.
     */
    public static final int TCP_PROTOCOL_VERSION_1 = 1;

    /**
     * The min TCP protocol version number.
     */
    public static final int TCP_PROTOCOL_VERSION_MIN = TCP_PROTOCOL_VERSION_1;

    /**
     * The max TCP protocol version number.
     */
    public static final int TCP_PROTOCOL_VERSION_MAX = TCP_PROTOCOL_VERSION_1;

    /**
     * The current TCP protocol version number.
     */
    public static final int TCP_PROTOCOL_VERSION_CURRENT = TCP_PROTOCOL_VERSION_1;

    /**
     * The number of milliseconds after which to check for a deadlock if locking
     * is not successful.
     */
    public static final int DEADLOCK_CHECK = 100;

    /**
     * Constant meaning both numbers and text is allowed in SQL statements.
     */
    public static final int ALLOW_LITERALS_ALL = 2;

    /**
     * Constant meaning no literals are allowed in SQL statements.
     */
    public static final int ALLOW_LITERALS_NONE = 0;

    /**
     * Constant meaning only numbers are allowed in SQL statements (but no texts).
     */
    public static final int ALLOW_LITERALS_NUMBERS = 1;

    /**
     * Whether searching in Blob values should be supported.
     */
    public static final boolean BLOB_SEARCH = false;

    /**
     * The cost is calculated on rowcount + this offset,
     * to avoid using the wrong or no index if the table
     * contains no rows _currently_ (when preparing the statement)
     */
    public static final int COST_ROW_OFFSET = 1000;

    /**
     * The default maximum length of an LOB that is stored in the database file.
     */
    public static final int DEFAULT_MAX_LENGTH_INPLACE_LOB = 4096;

    /**
     * The default maximum length of an LOB that is stored with the record itself,
     * and not in a separate place.
     * Only used if lealone.lobInDatabase is enabled.
     */
    public static final int DEFAULT_MAX_LENGTH_INPLACE_LOB2 = 128;

    /**
     * The default maximum number of rows to be kept in memory in a result set.
     */
    public static final int DEFAULT_MAX_MEMORY_ROWS = 10000;

    /**
     * The default value for the MAX_MEMORY_UNDO setting.
     */
    public static final int DEFAULT_MAX_MEMORY_UNDO = 50000;

    /**
     * The default for the setting MAX_OPERATION_MEMORY.
     */
    public static final int DEFAULT_MAX_OPERATION_MEMORY = 100000;

    /**
     * The default cache size in MB.
     */
    public static final int DEFAULT_CACHE_SIZE = 32;

    /**
     * The default page size to use for new databases.
     */
    public static final int DEFAULT_PAGE_SIZE = 16 * 1024; // 16K;

    /**
     * The default result set concurrency for statements created with
     * Connection.createStatement() or prepareStatement(String sql).
     */
    public static final int DEFAULT_RESULT_SET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;

    /**
     * The default port of the TCP server.
     */
    public static final int DEFAULT_TCP_PORT = 9210;

    /**
     * The default port of the P2P server.
     */
    public static final int DEFAULT_P2P_PORT = 9211;

    public static final String DEFAULT_HOST = "localhost";

    /**
     * The default delay in milliseconds before the transaction log is written.
     */
    public static final int DEFAULT_WRITE_DELAY = 500;

    /**
     * The password is hashed this many times
     * to slow down dictionary attacks.
     */
    public static final int ENCRYPTION_KEY_HASH_ITERATIONS = 1024;

    /**
     * The block of a file. It is also the encryption block size.
     */
    public static final int FILE_BLOCK_SIZE = 16;

    /**
     * For testing, the lock timeout is smaller than for interactive use cases.
     * This value could be increased to about 5 or 10 seconds.
     */
    public static final int INITIAL_LOCK_TIMEOUT = 2000;

    /**
     * The block size for I/O operations.
     */
    public static final int IO_BUFFER_SIZE = 4 * 1024;

    /**
     * The block size used to compress data in the LZFOutputStream.
     */
    public static final int IO_BUFFER_SIZE_COMPRESS = 128 * 1024;

    /**
     * The highest possible parameter index.
     */
    public static final int MAX_PARAMETER_INDEX = 100000;

    /**
     * The memory needed by a regular object with at least one field.
     */
    // Java 6, 64 bit: 24
    // Java 6, 32 bit: 12
    public static final int MEMORY_OBJECT = 24;

    /**
     * The memory needed by a pointer.
     */
    // Java 6, 64 bit: 8
    // Java 6, 32 bit: 4
    public static final int MEMORY_POINTER = 8;

    /**
     * The memory needed by a Row.
     */
    public static final int MEMORY_ROW = 40;

    /**
     * The name prefix used for indexes that are not explicitly named.
     */
    public static final String PREFIX_INDEX = "INDEX_";

    /**
     * The name prefix used for synthetic nested join tables.
     */
    public static final String PREFIX_JOIN = "SYSTEM_JOIN_";

    /**
     * The name prefix used for primary key constraints that are not explicitly
     * named.
     */
    public static final String PREFIX_PRIMARY_KEY = "PRIMARY_KEY_";

    /**
     * Every user belongs to this role.
     */
    public static final String PUBLIC_ROLE_NAME = "PUBLIC";

    /**
     * The number of bytes in random salt that is used to hash passwords.
     */
    public static final int SALT_LEN = 8;

    /**
     * The name of the default schema.
     */
    public static final String SCHEMA_MAIN = "PUBLIC";

    /**
     * The default selectivity (used if the selectivity is not calculated).
     */
    public static final int SELECTIVITY_DEFAULT = 50;

    /**
     * The number of distinct values to keep in memory when running ANALYZE.
     */
    public static final int SELECTIVITY_DISTINCT_COUNT = 10000;

    /**
     * Queries that take longer than this number of milliseconds are written to
     * the trace file with the level info.
     */
    public static final long SLOW_QUERY_LIMIT_MS = 100;

    /**
     * The database URL prefix of this database.
     */
    public static final String URL_PREFIX = "jdbc:lealone:";
    public static final String URL_TCP = "tcp:";
    public static final String URL_SSL = "ssl:";
    public static final String URL_EMBED = "embed:";

    public static final String JDBC_URL_KEY = "lealone.jdbc.url";

    /**
     * The database URL used when calling a function if only the column list
     * should be returned.
     */
    public static final String CONN_URL_COLUMNLIST = URL_PREFIX + "columnlist:connection";

    /**
     * The database URL used when calling a function if the data should be
     * returned.
     */
    public static final String CONN_URL_INTERNAL = URL_PREFIX + "default:connection";

    /**
     * The file name suffix of a database file.
     */
    public static final String SUFFIX_DB_FILE = ".db";

    /**
     * The file name suffix of temporary files.
     */
    public static final String SUFFIX_TEMP_FILE = ".temp.db";

    /**
     * The file name suffix of trace files.
     */
    public static final String SUFFIX_TRACE_FILE = ".trace.db";

    /**
     * The delay that is to be used if throttle has been enabled.
     */
    public static final int THROTTLE_DELAY = 50;

    /**
     * The database URL format in simplified Backus-Naur form.
     */
    public static final String URL_FORMAT = URL_PREFIX
            + "{ {embed:}dbName | {tcp|ssl}:[//] {server[:port][,server2[:port2]...]}/dbName }"
            + " {[;key=value...] | [?key=value][&key2=value2...]}";

    /**
     * The package name of user defined classes.
     */
    public static final String USER_PACKAGE = "org.lealone.dynamic";

    /**
     * Name of the character encoding format.
     */
    public static final Charset UTF8 = Charset.forName("UTF-8");

    /**
     * The maximum time in milliseconds to keep the cost of a view.
     * 10000 means 10 seconds.
     */
    public static final int VIEW_COST_CACHE_MAX_AGE = 10000;

    /**
     * The name of the index cache that is used for temporary view (subqueries used as tables).
     */
    public static final int VIEW_INDEX_CACHE_SIZE = 64;

    /**
     * The maximum number of entries in query statistics.
     */
    public static final int QUERY_STATISTICS_MAX_ENTRIES = 100;

    public static final boolean IS_WINDOWS = System.getProperty("os.name").startsWith("Windows");

    // 为了避免模块之间在编译期存在依赖，有些地方会用到反射，在这里统一定义类名
    public static final String REFLECTION_JDBC_CONNECTION = "org.lealone.client.jdbc.JdbcConnection";

    private Constants() {
        // utility class
    }

    /**
     * The major version of this database.
     */
    public static final int VERSION_MAJOR;

    /**
     * The minor version of this database.
     */
    public static final int VERSION_MINOR;

    /**
     * The build id is incremented for each public release.
     */
    public static final int BUILD_ID;

    /**
     * The build date is updated for each public release.
     */
    public static final String BUILD_DATE;

    public static final String RELEASE_VERSION;

    /**
     * Get the version of this product, consisting of major version, minor version, and build id.
     *
     * @return the version number
     */
    public static String getVersion() {
        return VERSION_MAJOR + "." + VERSION_MINOR + "." + BUILD_ID;
    }

    /**
     * Get the complete version number of this database, consisting of
     * the major version, the minor version, the build id, and the build date.
     *
     * @return the complete version
     */
    public static String getFullVersion() {
        return getVersion() + " (" + BUILD_DATE + ")";
    }

    private static String getProperty(Properties props, String key1, String key2) {
        String v = props.getProperty(key1);
        if (v == null && key2 != null) {
            v = System.getProperty(PROJECT_NAME_PREFIX + key2, "Unknown");
        }
        return v;
    }

    static {
        try {
            Properties props = Utils.getResourceAsProperties(RESOURCES_DIR + "version.properties");
            String releaseVersion = getProperty(props, "lealoneVersion", "lealone.version");
            BUILD_DATE = getProperty(props, "buildDate", "build.date");

            String v = releaseVersion;
            int pos = v.indexOf('-');
            if (pos > 0)
                v = v.substring(0, pos);
            String[] a = v.split("\\.");
            VERSION_MAJOR = Integer.parseInt(a[0]);
            VERSION_MINOR = Integer.parseInt(a[1]);
            BUILD_ID = Integer.parseInt(a[2]);
            RELEASE_VERSION = releaseVersion;
        } catch (Throwable e) {
            throw DbException.convert(e);
        }
    }
}
