/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import org.lealone.common.message.TraceSystem;
import org.lealone.common.util.MathUtils;
import org.lealone.common.util.Utils;

/**
 * The constants defined in this class are initialized from system properties.
 * Some system properties are per machine settings, and others are as a last
 * resort and temporary solution to work around a problem in the application or
 * database engine. Also, there are system properties to enable features that
 * are not yet fully tested or that are not backward compatible.
 * <p>
 * System properties can be set when starting the virtual machine:
 * </p>
 *
 * <pre>
 * java -Dlealone.base.dir=/temp
 * </pre>
 *
 * They can be set within the application, but this must be done before loading
 * any classes of this database (before loading the JDBC driver):
 *
 * <pre>
 * System.setProperty(&quot;lealone.base.dir&quot;, &quot;/temp&quot;);
 * </pre>
 * 
 * <p>
 * 除FILE_ENCODING、FILE_SEPARATOR、JAVA_SPECIFICATION_VERSION、LINE_SEPARATOR、USER_HOME这些标准属性外，
 * 其他属性都默认加"lealone."前缀
 * </p>
 *
 */
public class SysProperties {
    /**
     * INTERNAL
     */
    private static final String SCRIPT_DIRECTORY = "script.directory";

    /**
     * System property <code>file.encoding</code> (default: Cp1252).<br />
     * It is usually set by the system and is the default encoding used for the
     * RunScript and CSV tool.
     */
    public static final String FILE_ENCODING = Utils.getProperty("file.encoding", "Cp1252");

    /**
     * System property <code>file.separator</code> (default: /).<br />
     * It is usually set by the system, and used to build absolute file names.
     */
    public static final String FILE_SEPARATOR = Utils.getProperty("file.separator", "/");

    /**
     * System property <code>java.specification.version</code>.<br />
     * It is set by the system. Examples: 1.4, 1.5, 1.6.
     */
    public static final String JAVA_SPECIFICATION_VERSION = Utils.getProperty("java.specification.version", "1.4");

    /**
     * System property <code>line.separator</code> (default: \n).<br />
     * It is usually set by the system, and used by the script and trace tools.
     */
    public static final String LINE_SEPARATOR = Utils.getProperty("line.separator", "\n");

    /**
     * System property <code>user.home</code> (empty string if not set).<br />
     * It is usually set by the system, and used as a replacement for ~ in file
     * names.
     */
    public static final String USER_HOME = Utils.getProperty("user.home", "");

    /**
     * System property <code>allowed.classes</code> (default: *).<br />
     * Comma separated list of class names or prefixes.
     */
    public static final String ALLOWED_CLASSES = getProperty("allowed.classes", "*");

    /**
     * System property <code>enable.anonymous.ssl</code> (default: true).<br />
     * When using SSL connection, the anonymous cipher suite
     * SSL_DH_anon_WITH_RC4_128_MD5 should be enabled.
     */
    public static final boolean ENABLE_ANONYMOUS_SSL = getProperty("enable.anonymous.ssl", true);

    /**
     * System property <code>bind.address</code> (default: null).<br />
     * The bind address to use.
     */
    public static final String BIND_ADDRESS = getProperty("bind.address", null);

    /**
     * System property <code>check</code> (default: true).<br />
     * Assertions in the database engine.
     */
    // ## CHECK ##
    public static final boolean CHECK = getProperty("check", true);
    /*/
    public static final boolean CHECK = false;
    //*/

    /**
     * System property <code>check2</code> (default: true).<br />
     * Additional assertions in the database engine.
     */
    // ## CHECK ##
    public static final boolean CHECK2 = getProperty("check2", false);
    /*/
    public static final boolean CHECK2 = false;
    //*/

    /**
     * System property <code>client.trace.directory</code> (default:
     * trace.db/).<br />
     * Directory where the trace files of the JDBC client are stored (only for
     * client / server).
     */
    public static final String CLIENT_TRACE_DIRECTORY = getProperty("client.trace.directory", "trace.db/");

    /**
     * System property <code>collator.cache.size</code> (default: 32000).<br />
     * The cache size for collation keys (in elements). Used when a collator has
     * been set for the database.
     */
    public static final int COLLATOR_CACHE_SIZE = getProperty("collator.cache.size", 32000);

    /**
     * System property <code>datasource.trace.level</code> (default: 1).<br />
     * The trace level of the data source implementation. Default is 1 for
     * error.
     */
    public static final int DATASOURCE_TRACE_LEVEL = getProperty("datasource.trace.level", TraceSystem.ERROR);

    /**
     * System property <code>delay.wrong.password.min</code> (default: 250).<br />
     * The minimum delay in milliseconds before an exception is thrown for using
     * the wrong user name or password. This slows down brute force attacks. The
     * delay is reset to this value after a successful login. Unsuccessful
     * logins will double the time until DELAY_WRONG_PASSWORD_MAX.
     * To disable the delay, set this system property to 0.
     */
    public static final int DELAY_WRONG_PASSWORD_MIN = getProperty("delay.wrong.password.min", 250);

    /**
     * System property <code>delay.wrong.password.max</code> (default: 4000).<br />
     * The maximum delay in milliseconds before an exception is thrown for using
     * the wrong user name or password. This slows down brute force attacks. The
     * delay is reset after a successful login. The value 0 means there is no
     * maximum delay.
     */
    public static final int DELAY_WRONG_PASSWORD_MAX = getProperty("delay.wrong.password.max", 4000);

    /**
     * System property <code>lob.close.between.reads</code> (default: false).<br />
     * Close LOB files between read operations.
     */
    public static boolean LOB_CLOSE_BETWEEN_READS = getProperty("lob.close.between.reads", false);

    /**
     * System property <code>lob.files.per.directory</code> (default: 256).<br />
     * Maximum number of LOB files per directory.
     */
    public static final int LOB_FILES_PER_DIRECTORY = getProperty("lob.files.per.directory", 256);

    /**
     * System property <code>lob.in.database</code> (default: true).<br />
     * Store LOB files in the database.
     */
    public static final boolean LOB_IN_DATABASE = getProperty("lob.in.database", true);

    /**
     * System property <code>lob.client.max.size.memory</code> (default:
     * 1048576).<br />
     * The maximum size of a LOB object to keep in memory on the client side
     * when using the server mode.
     */
    public static final int LOB_CLIENT_MAX_SIZE_MEMORY = getProperty("lob.client.max.size.memory", 1024 * 1024);

    /**
     * System property <code>max.file.retry</code> (default: 16).<br />
     * Number of times to retry file delete and rename. in Windows, files can't
     * be deleted if they are open. Waiting a bit can help (sometimes the
     * Windows Explorer opens the files for a short time) may help. Sometimes,
     * running garbage collection may close files if the user forgot to call
     * Connection.close() or InputStream.close().
     */
    public static final int MAX_FILE_RETRY = Math.max(1, getProperty("max.file.retry", 16));

    /**
     * System property <code>max.trace.data.length</code> (default: 65535).<br />
     * The maximum size of a LOB value that is written as data to the trace system.
     */
    public static final long MAX_TRACE_DATA_LENGTH = getProperty("max.trace.data.length", 65535);

    /**
     * System property <code>nio.load.mapped</code> (default: false).<br />
     * If the mapped buffer should be loaded when the file is opened.
     * This can improve performance.
     */
    public static final boolean NIO_LOAD_MAPPED = getProperty("nio.load.mapped", false);

    /**
     * System property <code>nio.cleaner.hack</code> (default: false).<br />
     * If enabled, use the reflection hack to un-map the mapped file if
     * possible. If disabled, System.gc() is called in a loop until the object
     * is garbage collected. See also
     * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038
     */
    public static final boolean NIO_CLEANER_HACK = getProperty("nio.cleaner.hack", false);

    /**
     * System property <code>object.cache</code> (default: true).<br />
     * Cache commonly used values (numbers, strings). There is a shared cache
     * for all values.
     */
    public static final boolean OBJECT_CACHE = getProperty("object.cache", true);

    /**
     * System property <code>object.cache.max.per.element.size</code> (default:
     * 4096).<br />
     * The maximum size (precision) of an object in the cache.
     */
    public static final int OBJECT_CACHE_MAX_PER_ELEMENT_SIZE = getProperty("object.cache.max.per.element.size", 4096);

    /**
     * System property <code>object.cache.size</code> (default: 1024).<br />
     * The maximum number of objects in the cache.
     * This value must be a power of 2.
     */
    public static final int OBJECT_CACHE_SIZE = MathUtils.nextPowerOf2(getProperty("object.cache.size", 1024));

    /**
     * System property <code>pg.client.encoding</code> (default: UTF-8).<br />
     * Default client encoding for PG server. It is used if the client does not
     * sends his encoding.
     */
    public static final String PG_DEFAULT_CLIENT_ENCODING = getProperty("pg.client.encoding", "UTF-8");

    /**
     * System property <code>prefix.temp.file</code> (default: lealone.temp).<br />
     * The prefix for temporary files in the temp directory.
     */
    public static final String PREFIX_TEMP_FILE = getProperty("prefix.temp.file", Constants.PROJECT_NAME_PREFIX
            + "temp");

    /**
     * System property <code>server.cached.objects</code> (default: 64).<br />
     * TCP Server: number of cached objects per session.
     */
    public static final int SERVER_CACHED_OBJECTS = getProperty("server.cached.objects", 64);

    /**
     * System property <code>server.resultset.fetch.size</code>
     * (default: 100).<br />
     * The default result set fetch size when using the server mode.
     */
    public static final int SERVER_RESULT_SET_FETCH_SIZE = getProperty("server.resultset.fetch.size", 100);

    /**
     * System property <code>socket.connect.retry</code> (default: 16).<br />
     * The number of times to retry opening a socket. Windows sometimes fails
     * to open a socket, see bug
     * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6213296
     */
    public static final int SOCKET_CONNECT_RETRY = getProperty("socket.connect.retry", 16);

    /**
     * System property <code>socket.connect.timeout</code> (default: 2000).<br />
     * The timeout in milliseconds to connect to a server.
     */
    public static final int SOCKET_CONNECT_TIMEOUT = getProperty("socket.connect.timeout", 2000);

    /**
     * System property <code>sort.nulls.high</code> (default: false).<br />
     * Invert the default sorting behavior for NULL, such that NULL
     * is at the end of a result set in an ascending sort and at
     * the beginning of a result set in a descending sort.
     */
    public static final boolean SORT_NULLS_HIGH = getProperty("sort.nulls.high", false);

    /**
     * System property <code>split.file.size.shift</code> (default: 30).<br />
     * The maximum file size of a split file is 1L &lt;&lt; x.
     */
    public static final long SPLIT_FILE_SIZE_SHIFT = getProperty("split.file.size.shift", 30);

    /**
     * System property <code>store.local.time</code> (default: false).<br />
     * Store the local time. If disabled, the daylight saving offset is not
     * taken into account.
     */
    public static final boolean STORE_LOCAL_TIME = getProperty("store.local.time", false);

    /**
     * System property <code>sync.method</code> (default: sync).<br />
     * What method to call when closing the database, on checkpoint, and on
     * CHECKPOINT SYNC. The following options are supported:
     * "sync" (default): RandomAccessFile.getFD().sync();
     * "force": RandomAccessFile.getChannel().force(true);
     * "forceFalse": RandomAccessFile.getChannel().force(false);
     * "": do not call a method (fast but there is a risk of data loss
     * on power failure).
     */
    public static final String SYNC_METHOD = getProperty("sync.method", "sync");

    /**
     * System property <code>trace.io</code> (default: false).<br />
     * Trace all I/O operations.
     */
    public static final boolean TRACE_IO = getProperty("trace.io", false);

    /**
     * System property <code>url.map</code> (default: null).<br />
     * A properties file that contains a mapping between database URLs. New
     * connections are written into the file. An empty value in the map means no
     * redirection is used for the given URL.
     */
    public static final String URL_MAP = getProperty("url.map", null);

    /**
     * System property <code>implicit.relative.path</code>
     * (default: true for version 1.3, false for version 1.4).<br />
     * If disabled, relative paths in database URLs need to be written as
     * jdbc:h2:./test instead of jdbc:h2:test.
     */
    // public static final boolean IMPLICIT_RELATIVE_PATH = getProperty("implicit.relative.path",
    // Constants.VERSION_MINOR >= 4 ? false : true);
    /**
     * System property <code>use.thread.context.classloader</code>
     * (default: false).<br />
     * Instead of using the default class loader when deserializing objects, the
     * current thread-context class loader will be used.
     */
    public static final boolean USE_THREAD_CONTEXT_CLASS_LOADER = getProperty("use.thread.context.classloader", false);

    /**
     * System property <code>serialize.java.object</code> (default: true).<br />
     * <b>If true</b>, values of type OTHER will be stored in serialized form
     * and have the semantics of binary data for all operations (such as sorting
     * and conversion to string).
     * <br />
     * <b>If false</b>, the objects will be serialized only for I/O operations
     * and a few other special cases (for example when someone tries to get the
     * value in binary form or when comparing objects that are not comparable
     * otherwise).
     * <br />
     * If the object implements the Comparable interface, the method compareTo
     * will be used for sorting (but only if objects being compared have a
     * common comparable super type). Otherwise the objects will be compared by
     * type, and if they are the same by hashCode, and if the hash codes are
     * equal, but objects are not, the serialized forms (the byte arrays) are
     * compared.
     * <br />
     * The string representation of the values use the toString method of
     * object.
     * <br />
     * In client-server mode, the server must have all required classes in the
     * class path. On the client side, this setting is required to be disabled
     * as well, to have correct string representation and display size.
     * <br />
     * In embedded mode, no data copying occurs, so the user has to make
     * defensive copy himself before storing, or ensure that the value object is
     * immutable.
     */
    public static boolean SERIALIZE_JAVA_OBJECT = getProperty("serialize.java.object", true);

    /**
     * System property <code>java.object.serializer</code> (default: null).<br />
     * The JavaObjectSerializer class name for java objects being stored in
     * column of type OTHER. It must be the same on client and server to work
     * correctly.
     */
    public static final String JAVA_OBJECT_SERIALIZER = getProperty("java.object.serializer", null);

    /**
     * System property <code>old.style.outer.join</code>
     * (default: false).<br />
     * Limited support for the old-style Oracle outer join with "(+)".
     */
    public static final boolean OLD_STYLE_OUTER_JOIN = Utils.getProperty("old.style.outer.join", false);

    private static final String BASE_DIR = "base.dir";

    private SysProperties() {
        // utility class
    }

    /**
     * INTERNAL
     */
    public static String getBaseDir() {
        String dir = getProperty(BASE_DIR, null);
        if (dir == null)
            throw new RuntimeException("Fatal error: base dir is null, the system property '"
                    + Constants.PROJECT_NAME_PREFIX + BASE_DIR + "' must be set");
        return dir;
    }

    public static String getBaseDirSilently() {
        return getProperty(BASE_DIR, null);
    }

    /**
     * INTERNAL
     */
    public static String setBaseDir(String baseDir) {
        return System.setProperty(Constants.PROJECT_NAME_PREFIX + BASE_DIR, baseDir);
    }

    /**
     * System property <code>scriptDirectory</code> (default: empty
     * string).<br />
     * Relative or absolute directory where the script files are stored to or
     * read from.
     *
     * @return the current value
     */
    public static String getScriptDirectory() {
        return getProperty(SCRIPT_DIRECTORY, "");
    }

    private static String getProperty(String key, String defaultValue) {
        return Utils.getProperty(Constants.PROJECT_NAME_PREFIX + key, defaultValue);
    }

    private static int getProperty(String key, int defaultValue) {
        return Utils.getProperty(Constants.PROJECT_NAME_PREFIX + key, defaultValue);
    }

    private static boolean getProperty(String key, boolean defaultValue) {
        return Utils.getProperty(Constants.PROJECT_NAME_PREFIX + key, defaultValue);
    }
}
