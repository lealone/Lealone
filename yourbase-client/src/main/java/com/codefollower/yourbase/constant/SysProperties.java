/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.yourbase.constant;

import com.codefollower.yourbase.message.TraceSystem;
import com.codefollower.yourbase.util.MathUtils;
import com.codefollower.yourbase.util.Utils;

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
 * java -Dyourbase.base.dir=/temp
 * </pre>
 *
 * They can be set within the application, but this must be done before loading
 * any classes of this database (before loading the JDBC driver):
 *
 * <pre>
 * System.setProperty(&quot;yourbase.base.dir&quot;, &quot;/temp&quot;);
 * </pre>
 */
public class SysProperties {

    /**
     * INTERNAL
     */
    public static final String YOURBASE_SCRIPT_DIRECTORY = "yourbase.scriptDirectory";

    /**
     * INTERNAL
     */
    public static final String YOURBASE_BROWSER = "yourbase.browser";

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
     * System property <code>yourbase.allowed.classes</code> (default: *).<br />
     * Comma separated list of class names or prefixes.
     */
    public static final String ALLOWED_CLASSES = Utils.getProperty("yourbase.allowed.classes", "*");

    /**
     * System property <code>yourbase.browser</code> (default: null).<br />
     * The preferred browser to use. If not set, the default browser is used.
     * For Windows, to use the Internet Explorer, set this property to 'explorer'.
     * For Mac OS, if the default browser is not Safari and you want to use Safari,
     * use: <code>java -Dh2.browser="open,-a,Safari,%url" ...</code>.
     */
    public static final String BROWSER = Utils.getProperty(YOURBASE_BROWSER, null);

    /**
     * System property <code>yourbase.enable.anonymous.ssl</code> (default: true).<br />
     * When using SSL connection, the anonymous cipher suite
     * SSL_DH_anon_WITH_RC4_128_MD5 should be enabled.
     */
    public static final boolean ENABLE_ANONYMOUS_SSL = Utils.getProperty("yourbase.enable.anonymous.ssl", true);

    /**
     * System property <code>yourbase.bind.address</code> (default: null).<br />
     * The bind address to use.
     */
    public static final String BIND_ADDRESS = Utils.getProperty("yourbase.bind.address", null);

    /**
     * System property <code>yourbase.check</code> (default: true).<br />
     * Assertions in the database engine.
     */
    //## CHECK ##
    public static final boolean CHECK = Utils.getProperty("yourbase.check", true);
    /*/
    public static final boolean CHECK = false;
    //*/

    /**
     * System property <code>yourbase.check2</code> (default: true).<br />
     * Additional assertions in the database engine.
     */
    //## CHECK ##
    public static final boolean CHECK2 = Utils.getProperty("yourbase.check2", false);
    /*/
    public static final boolean CHECK2 = false;
    //*/

    /**
     * System property <code>yourbase.client.trace.directory</code> (default:
     * trace.db/).<br />
     * Directory where the trace files of the JDBC client are stored (only for
     * client / server).
     */
    public static final String CLIENT_TRACE_DIRECTORY = Utils.getProperty("yourbase.client.trace.directory", "trace.db/");

    /**
     * System property <code>yourbase.collator.cache.size</code> (default: 32000).<br />
     * The cache size for collation keys (in elements). Used when a collator has
     * been set for the database.
     */
    public static final int COLLATOR_CACHE_SIZE = Utils.getProperty("yourbase.collator.cache.size", 32000);

    /**
     * System property <code>yourbase.console.stream</code> (default: true).<br />
     * H2 Console: stream query results.
     */
    public static final boolean CONSOLE_STREAM = Utils.getProperty("yourbase.console.stream", true);

    /**
     * System property <code>yourbase.datasource.trace.level</code> (default: 1).<br />
     * The trace level of the data source implementation. Default is 1 for
     * error.
     */
    public static final int DATASOURCE_TRACE_LEVEL = Utils.getProperty("yourbase.datasource.trace.level", TraceSystem.ERROR);

    /**
     * System property <code>yourbase.delay.wrong.password.min</code> (default: 250).<br />
     * The minimum delay in milliseconds before an exception is thrown for using
     * the wrong user name or password. This slows down brute force attacks. The
     * delay is reset to this value after a successful login. Unsuccessful
     * logins will double the time until DELAY_WRONG_PASSWORD_MAX.
     * To disable the delay, set this system property to 0.
     */
    public static final int DELAY_WRONG_PASSWORD_MIN = Utils.getProperty("yourbase.delay.wrong.password.min", 250);

    /**
     * System property <code>yourbase.delay.wrong.password.max</code> (default: 4000).<br />
     * The maximum delay in milliseconds before an exception is thrown for using
     * the wrong user name or password. This slows down brute force attacks. The
     * delay is reset after a successful login. The value 0 means there is no
     * maximum delay.
     */
    public static final int DELAY_WRONG_PASSWORD_MAX = Utils.getProperty("yourbase.delay.wrong.password.max", 4000);

    /**
     * System property <code>yourbase.lob.close.between.reads</code> (default: false).<br />
     * Close LOB files between read operations.
     */
    public static boolean lobCloseBetweenReads = Utils.getProperty("yourbase.lob.close.between.reads", false);

    /**
     * System property <code>yourbase.lob.files.per.directory</code> (default: 256).<br />
     * Maximum number of LOB files per directory.
     */
    public static final int LOB_FILES_PER_DIRECTORY = Utils.getProperty("yourbase.lob.files.per.directory", 256);

    /**
     * System property <code>yourbase.lob.in.database</code> (default: true).<br />
     * Store LOB files in the database.
     */
    public static final boolean LOB_IN_DATABASE = Utils.getProperty("yourbase.lob.in.database", true);

    /**
     * System property <code>yourbase.lob.client.max.size.memory</code> (default:
     * 1048576).<br />
     * The maximum size of a LOB object to keep in memory on the client side
     * when using the server mode.
     */
    public static final int LOB_CLIENT_MAX_SIZE_MEMORY = Utils.getProperty("yourbase.lob.client.max.size.memory", 1024 * 1024);

    /**
     * System property <code>yourbase.max.file.retry</code> (default: 16).<br />
     * Number of times to retry file delete and rename. in Windows, files can't
     * be deleted if they are open. Waiting a bit can help (sometimes the
     * Windows Explorer opens the files for a short time) may help. Sometimes,
     * running garbage collection may close files if the user forgot to call
     * Connection.close() or InputStream.close().
     */
    public static final int MAX_FILE_RETRY = Math.max(1, Utils.getProperty("yourbase.max.file.retry", 16));

    /**
     * System property <code>yourbase.max.reconnect</code> (default: 3).<br />
     * The maximum number of tries to reconnect in a row.
     */
    public static final int MAX_RECONNECT = Utils.getProperty("yourbase.max.reconnect", 3);

    /**
     * System property <code>yourbase.max.trace.data.length</code> (default: 65535).<br />
     * The maximum size of a LOB value that is written as data to the trace system.
     */
    public static final long MAX_TRACE_DATA_LENGTH = Utils.getProperty("yourbase.max.trace.data.length", 65535);

    /**
     * System property <code>yourbase.modify.on.write</code> (default: false).<br />
     * Only modify the database file when recovery is necessary, or when writing
     * to the database. If disabled, opening the database always writes to the
     * file (except if the database is read-only). When enabled, the serialized
     * file lock is faster.
     */
    public static final boolean MODIFY_ON_WRITE = Utils.getProperty("yourbase.modify.on.write", false);

    /**
     * System property <code>yourbase.nio.load.mapped</code> (default: false).<br />
     * If the mapped buffer should be loaded when the file is opened.
     * This can improve performance.
     */
    public static final boolean NIO_LOAD_MAPPED = Utils.getProperty("yourbase.nio.load.mapped", false);

    /**
     * System property <code>yourbase.nio.cleaner.hack</code> (default: false).<br />
     * If enabled, use the reflection hack to un-map the mapped file if
     * possible. If disabled, System.gc() is called in a loop until the object
     * is garbage collected. See also
     * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038
     */
    public static final boolean NIO_CLEANER_HACK = Utils.getProperty("yourbase.nio.cleaner.hack", false);

    /**
     * System property <code>yourbase.object.cache</code> (default: true).<br />
     * Cache commonly used values (numbers, strings). There is a shared cache
     * for all values.
     */
    public static final boolean OBJECT_CACHE = Utils.getProperty("yourbase.object.cache", true);

    /**
     * System property <code>yourbase.object.cache.max.per.element.size</code> (default:
     * 4096).<br />
     * The maximum size (precision) of an object in the cache.
     */
    public static final int OBJECT_CACHE_MAX_PER_ELEMENT_SIZE = Utils.getProperty("yourbase.object.cache.max.per.element.size",
            4096);

    /**
     * System property <code>yourbase.object.cache.size</code> (default: 1024).<br />
     * The maximum number of objects in the cache.
     * This value must be a power of 2.
     */
    public static final int OBJECT_CACHE_SIZE = MathUtils.nextPowerOf2(Utils.getProperty("yourbase.object.cache.size", 1024));

    /**
     * System property <code>yourbase.pg.client.encoding</code> (default: UTF-8).<br />
     * Default client encoding for PG server. It is used if the client does not
     * sends his encoding.
     */
    public static final String PG_DEFAULT_CLIENT_ENCODING = Utils.getProperty("yourbase.pg.client.encoding", "UTF-8");

    /**
     * System property <code>yourbase.prefix.temp.file</code> (default: yourbase.temp).<br />
     * The prefix for temporary files in the temp directory.
     */
    public static final String PREFIX_TEMP_FILE = Utils.getProperty("yourbase.prefix.temp.file", "yourbase.temp");

    /**
     * System property <code>yourbase.run.finalize</code> (default: true).<br />
     * Run finalizers to detect unclosed connections.
     */
    public static boolean runFinalize = Utils.getProperty("yourbase.run.finalize", true);

    /**
     * System property <code>yourbase.server.cached.objects</code> (default: 64).<br />
     * TCP Server: number of cached objects per session.
     */
    public static final int SERVER_CACHED_OBJECTS = Utils.getProperty("yourbase.server.cached.objects", 64);

    /**
     * System property <code>yourbase.server.resultset.fetch.size</code>
     * (default: 100).<br />
     * The default result set fetch size when using the server mode.
     */
    public static final int SERVER_RESULT_SET_FETCH_SIZE = Utils.getProperty("yourbase.server.resultset.fetch.size", 100);

    /**
     * System property <code>yourbase.socket.connect.retry</code> (default: 16).<br />
     * The number of times to retry opening a socket. Windows sometimes fails
     * to open a socket, see bug
     * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6213296
     */
    public static final int SOCKET_CONNECT_RETRY = Utils.getProperty("yourbase.socket.connect.retry", 16);

    /**
     * System property <code>yourbase.socket.connect.timeout</code> (default: 2000).<br />
     * The timeout in milliseconds to connect to a server.
     */
    public static final int SOCKET_CONNECT_TIMEOUT = Utils.getProperty("yourbase.socket.connect.timeout", 2000);

    /**
     * System property <code>yourbase.sort.nulls.high</code> (default: false).<br />
     * Invert the default sorting behavior for NULL, such that NULL
     * is at the end of a result set in an ascending sort and at
     * the beginning of a result set in a descending sort.
     */
    public static final boolean SORT_NULLS_HIGH = Utils.getProperty("yourbase.sort.nulls.high", false);

    /**
     * System property <code>yourbase.split.file.size.shift</code> (default: 30).<br />
     * The maximum file size of a split file is 1L &lt;&lt; x.
     */
    public static final long SPLIT_FILE_SIZE_SHIFT = Utils.getProperty("yourbase.split.file.size.shift", 30);

    /**
     * System property <code>yourbase.store.local.time</code> (default: false).<br />
     * Store the local time. If disabled, the daylight saving offset is not
     * taken into account.
     */
    public static final boolean STORE_LOCAL_TIME = Utils.getProperty("yourbase.store.local.time", false);

    /**
     * System property <code>yourbase.sync.method</code> (default: sync).<br />
     * What method to call when closing the database, on checkpoint, and on
     * CHECKPOINT SYNC. The following options are supported:
     * "sync" (default): RandomAccessFile.getFD().sync();
     * "force": RandomAccessFile.getChannel().force(true);
     * "forceFalse": RandomAccessFile.getChannel().force(false);
     * "": do not call a method (fast but there is a risk of data loss
     * on power failure).
     */
    public static final String SYNC_METHOD = Utils.getProperty("yourbase.sync.method", "sync");

    /**
     * System property <code>yourbase.trace.io</code> (default: false).<br />
     * Trace all I/O operations.
     */
    public static final boolean TRACE_IO = Utils.getProperty("yourbase.trace.io", false);

    /**
     * System property <code>yourbase.url.map</code> (default: null).<br />
     * A properties file that contains a mapping between database URLs. New
     * connections are written into the file. An empty value in the map means no
     * redirection is used for the given URL.
     */
    public static final String URL_MAP = Utils.getProperty("yourbase.url.map", null);

    /**
     * System property <code>yourbase.use.thread.context.classloader</code>
     * (default: false).<br />
     * Instead of using the default class loader when deserializing objects, the
     * current thread-context class loader will be used.
     */
    public static final boolean USE_THREAD_CONTEXT_CLASS_LOADER = Utils.getProperty("yourbase.use.thread.context.classloader",
            false);

    /**
     * System property <code>yourbase.serialize.java.object</code> (default: true).<br />
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
    public static boolean serializeJavaObject = Utils.getProperty("yourbase.serialize.java.object", true);

    /**
     * System property <code>yourbase.java.object.serializer</code> (default: null).<br />
     * The JavaObjectSerializer class name for java objects being stored in
     * column of type OTHER. It must be the same on client and server to work
     * correctly.
     */
    public static final String JAVA_OBJECT_SERIALIZER = Utils.getProperty("yourbase.java.object.serializer", null);

    private static final String YOURBASE_BASE_DIR = "yourbase.base.dir";

    private SysProperties() {
        // utility class
    }

    /**
     * INTERNAL
     */
    public static void setBaseDir(String dir) {
        if (!dir.endsWith("/")) {
            dir += "/";
        }
        System.setProperty(YOURBASE_BASE_DIR, dir);
    }

    /**
     * INTERNAL
     */
    public static String getBaseDir() {
        return Utils.getProperty(YOURBASE_BASE_DIR, null);
    }

    /**
     * System property <code>yourbase.scriptDirectory</code> (default: empty
     * string).<br />
     * Relative or absolute directory where the script files are stored to or
     * read from.
     *
     * @return the current value
     */
    public static String getScriptDirectory() {
        return Utils.getProperty(YOURBASE_SCRIPT_DIRECTORY, "");
    }

}
