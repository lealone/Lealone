/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.lealone.api.ErrorCode;
import org.lealone.common.message.DbException;
import org.lealone.common.util.BitField;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.IOUtils;
import org.lealone.common.util.New;
import org.lealone.db.Constants;
import org.lealone.db.DatabaseEngine;
import org.lealone.db.InDoubtTransaction;
import org.lealone.db.Session;
import org.lealone.db.SessionInterface;
import org.lealone.db.index.ValueDataType;
import org.lealone.db.table.MVTable;
import org.lealone.db.table.Table;
import org.lealone.mvstore.MVMap;
import org.lealone.mvstore.MVStore;
import org.lealone.mvstore.MVStoreTool;
import org.lealone.storage.fs.FileChannelInputStream;
import org.lealone.storage.fs.FileUtils;
import org.lealone.storage.type.DataType;
import org.lealone.transaction.MVCCTransactionEngine;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionMap;

/**
 * A storage engine that internally uses the MVStore.
 */
public class MVStorageEngine extends StorageEngineBase implements TransactionStorageEngine {
    public static final String NAME = "MVStore";
    private static HashMap<String, Store> stores = new HashMap<>(1);

    // 见StorageEngineManager.StorageEngineService中的注释
    public MVStorageEngine() {
        StorageEngineManager.registerStorageEngine(this);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public synchronized Table createTable(CreateTableData data0) {
        org.lealone.db.table.CreateTableData data = (org.lealone.db.table.CreateTableData) data0;
        org.lealone.db.Database db = data.session.getDatabase();
        Store store = stores.get(db.getName());
        if (store == null) {
            store = init(this, db);
            stores.put(db.getName(), store);
        }

        MVTable table = new MVTable(data, this);
        table.init(data.session);
        store.tableMap.put(table.getMapName(), table);
        return table;
    }

    @Override
    public synchronized void close(Database db0) {
        org.lealone.db.Database db = (org.lealone.db.Database) db0;
        stores.remove(db.getName());
    }

    @Override
    public synchronized StorageMap.Builder createStorageMapBuilder(String dbName) {
        return new MVMapBuilder(stores.get(dbName).getStore());
    }

    @Override
    public TransactionEngine createTransactionEngine(DataType dataType, StorageMap.Builder mapBuilder,
            String hostAndPort) {
        return new MVCCTransactionEngine(dataType, mapBuilder, hostAndPort, Session.isClusterMode());
    }

    public static Store getStore(Session session) {
        return getStore(session.getDatabase());
    }

    public static Store getStore(Database db0) {
        org.lealone.db.Database db = (org.lealone.db.Database) db0;
        return stores.get(db.getName());
    }

    /**
     * Initialize the MVStore.
     *
     * @param db the database
     * @return the store
     */
    static Store init(StorageEngine storageEngine, final Database db0) {
        final org.lealone.db.Database db = (org.lealone.db.Database) db0;
        Store store = null;
        byte[] key = db.getFileEncryptionKey();
        String dbPath = db.getDatabasePath();
        MVStore.Builder builder = new MVStore.Builder();
        if (dbPath == null) {
            store = new Store(storageEngine, db, builder);
        } else {
            String fileName = dbPath + Constants.SUFFIX_MV_FILE;
            builder.pageSplitSize(db.getPageSize());
            MVStoreTool.compactCleanUp(fileName);
            builder.fileName(fileName);
            if (db.isReadOnly()) {
                builder.readOnly();
            } else {
                // possibly create the directory
                boolean exists = FileUtils.exists(fileName);
                if (exists && !FileUtils.canWrite(fileName)) {
                    // read only
                } else {
                    String dir = FileUtils.getParent(fileName);
                    FileUtils.createDirectories(dir);
                }
            }
            if (key != null) {
                char[] password = new char[key.length / 2];
                for (int i = 0; i < password.length; i++) {
                    password[i] = (char) (((key[i + i] & 255) << 16) | ((key[i + i + 1]) & 255));
                }
                builder.encryptionKey(password);
            }
            if (db.getSettings().compressData) {
                builder.compress();
                // use a larger page split size to improve the compression ratio
                builder.pageSplitSize(64 * 1024);
            }
            builder.backgroundExceptionHandler(new UncaughtExceptionHandler() {

                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    db.setBackgroundException(DbException.convert(e));
                }

            });
            try {
                store = new Store(storageEngine, db, builder);
            } catch (IllegalStateException e) {
                int errorCode = DataUtils.getErrorCode(e.getMessage());
                if (errorCode == DataUtils.ERROR_FILE_CORRUPT) {
                    if (key != null) {
                        throw DbException.get(ErrorCode.FILE_ENCRYPTION_ERROR_1, e, fileName);
                    }
                } else if (errorCode == DataUtils.ERROR_FILE_LOCKED) {
                    throw DbException.get(ErrorCode.DATABASE_ALREADY_OPEN_1, e, fileName);
                } else if (errorCode == DataUtils.ERROR_READING_FAILED) {
                    throw DbException.get(ErrorCode.IO_EXCEPTION_1, e, fileName);
                }
                throw DbException.get(ErrorCode.FILE_CORRUPTED_1, e, fileName);
            }
        }
        return store;
    }

    /**
     * A store with open tables.
     */
    public static class Store {

        /**
         * The map of open tables.
         * Key: the map name, value: the table.
         */
        final ConcurrentHashMap<String, MVTable> tableMap = new ConcurrentHashMap<>();

        /**
         * The store.
         */
        private final MVStore store;

        /**
         * The transaction engine.
         */
        private final TransactionEngine transactionEngine;

        private long statisticsStart;

        private int temporaryMapId;

        public Store(StorageEngine storageEngine, Database db0, MVStore.Builder builder) {
            org.lealone.db.Database db = (org.lealone.db.Database) db0;
            store = builder.open();

            stores.put(db.getName(), this);

            StorageMap.Builder mapBuilder = storageEngine.createStorageMapBuilder(db.getName());
            transactionEngine = storageEngine.createTransactionEngine(new ValueDataType(null, db, null), mapBuilder,
                    DatabaseEngine.getHostAndPort());

            transactionEngine.init(store.getMapNames());
            // 不能过早初始化，需要等执行完MetaRecord之后生成所有Map了才行，
            // 否则在执行undo log时对应map的sortTypes是null，在执行ValueDataType.compare(Object, Object)时出现空指针异常
            // initTransactions();
            db.setTransactionEngine(transactionEngine);
            db.addStorageEngine(storageEngine);
            db.setLobStorage(new LobStorageMap(db));
        }

        public MVStore getStore() {
            return store;
        }

        public HashMap<String, MVTable> getTables() {
            return new HashMap<String, MVTable>(tableMap);
        }

        /**
         * Remove a table.
         *
         * @param table the table
         */
        public void removeTable(MVTable table) {
            tableMap.remove(table.getMapName());
        }

        /**
         * Store all pending changes.
         */
        public void flush() {
            org.lealone.mvstore.FileStore s = store.getFileStore();
            if (s == null || s.isReadOnly()) {
                return;
            }
            if (!store.compact(50, 4 * 1024 * 1024)) {
                store.commit();
            }
        }

        /**
         * Close the store, without persisting changes.
         */
        public void closeImmediately() {
            if (store.isClosed()) {
                return;
            }
            store.closeImmediately();
        }

        /**
         * Commit all transactions that are in the committing state, and
         * rollback all open transactions.
         */
        public void initTransactions() {
            List<Transaction> list = transactionEngine.getOpenTransactions();
            for (Transaction t : list) {
                if (t.getStatus() == Transaction.STATUS_COMMITTING) {
                    t.commit();
                } else if (t.getStatus() != Transaction.STATUS_PREPARED) {
                    t.rollback();
                }
            }
        }

        /**
         * Remove all temporary maps.
         *
         * @param objectIds the ids of the objects to keep
         */
        public void removeTemporaryMaps(BitField objectIds) {
            for (String mapName : store.getMapNames()) {
                if (mapName.startsWith("temp.")) {
                    MVMap<?, ?> map = store.openMap(mapName);
                    store.removeMap(map);
                } else if (mapName.startsWith("table.") || mapName.startsWith("index.")) {
                    int id = Integer.parseInt(mapName.substring(1 + mapName.indexOf(".")));
                    if (!objectIds.get(id)) {
                        ValueDataType keyType = new ValueDataType(null, null, null);
                        ValueDataType valueType = new ValueDataType(null, null, null);
                        Transaction t = transactionEngine.beginTransaction(false);
                        TransactionMap<?, ?> m = t.openMap(mapName, keyType, valueType);
                        transactionEngine.removeMap(m);
                        t.commit();
                    }
                }
            }
        }

        /**
         * Get the name of the next available temporary map.
         *
         * @return the map name
         */
        public synchronized String nextTemporaryMapName() {
            return "temp." + temporaryMapId++;
        }

        /**
         * Prepare a transaction.
         *
         * @param session the session
         * @param transactionName the transaction name (may be null)
         */
        public void prepareCommit(Session session, String transactionName) {
            Transaction t = session.getTransaction();
            t.setName(transactionName);
            t.prepare();
            store.commit();
        }

        public ArrayList<InDoubtTransaction> getInDoubtTransactions() {
            List<Transaction> list = transactionEngine.getOpenTransactions();
            ArrayList<InDoubtTransaction> result = New.arrayList();
            for (Transaction t : list) {
                if (t.getStatus() == Transaction.STATUS_PREPARED) {
                    result.add(new MVInDoubtTransaction(store, t));
                }
            }
            return result;
        }

        /**
         * Set the maximum memory to be used by the cache.
         *
         * @param kb the maximum size in KB
         */
        public void setCacheSize(int kb) {
            store.setCacheSize(Math.max(1, kb / 1024));
        }

        public InputStream getInputStream() {
            FileChannel fc = store.getFileStore().getEncryptedFile();
            if (fc == null) {
                fc = store.getFileStore().getFile();
            }
            return new FileChannelInputStream(fc, false);
        }

        /**
         * Force the changes to disk.
         */
        public void sync() {
            flush();
            store.sync();
        }

        /**
         * Compact the database file, that is, compact blocks that have a low
         * fill rate, and move chunks next to each other. This will typically
         * shrink the database file. Changes are flushed to the file, and old
         * chunks are overwritten.
         *
         * @param maxCompactTime the maximum time in milliseconds to compact
         */
        public void compactFile(long maxCompactTime) {
            store.setRetentionTime(0);
            long start = System.currentTimeMillis();
            while (store.compact(95, 16 * 1024 * 1024)) {
                store.sync();
                store.compactMoveChunks(95, 16 * 1024 * 1024);
                long time = System.currentTimeMillis() - start;
                if (time > maxCompactTime) {
                    break;
                }
            }
        }

        /**
         * Close the store. Pending changes are persisted. Chunks with a low
         * fill rate are compacted, but old chunks are kept for some time, so
         * most likely the database file will not shrink.
         *
         * @param maxCompactTime the maximum time in milliseconds to compact
         */
        public void close(long maxCompactTime) {
            try {
                if (!store.isClosed() && store.getFileStore() != null) {
                    boolean compactFully = false;
                    if (!store.getFileStore().isReadOnly()) {
                        transactionEngine.close();
                        if (maxCompactTime == Long.MAX_VALUE) {
                            compactFully = true;
                        }
                    }
                    String fileName = store.getFileStore().getFileName();
                    store.close();
                    if (compactFully && FileUtils.exists(fileName)) {
                        // the file could have been deleted concurrently,
                        // so only compact if the file still exists
                        MVStoreTool.compact(fileName, true);
                    }
                }
            } catch (IllegalStateException e) {
                int errorCode = DataUtils.getErrorCode(e.getMessage());
                if (errorCode == DataUtils.ERROR_WRITING_FAILED) {
                    // disk full - ok
                } else if (errorCode == DataUtils.ERROR_FILE_CORRUPT) {
                    // wrong encryption key - ok
                }
                store.closeImmediately();
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, e, "Closing");
            }
        }

        /**
         * Start collecting statistics.
         */
        public void statisticsStart() {
            org.lealone.mvstore.FileStore fs = store.getFileStore();
            statisticsStart = fs == null ? 0 : fs.getReadCount();
        }

        /**
         * Stop collecting statistics.
         *
         * @return the statistics
         */
        public Map<String, Integer> statisticsEnd() {
            HashMap<String, Integer> map = New.hashMap();
            org.lealone.mvstore.FileStore fs = store.getFileStore();
            int reads = fs == null ? 0 : (int) (fs.getReadCount() - statisticsStart);
            map.put("reads", reads);
            return map;
        }

    }

    /**
     * An in-doubt transaction.
     */
    private static class MVInDoubtTransaction implements InDoubtTransaction {

        private final MVStore store;
        private final Transaction transaction;
        private int state = InDoubtTransaction.IN_DOUBT;

        MVInDoubtTransaction(MVStore store, Transaction transaction) {
            this.store = store;
            this.transaction = transaction;
        }

        @Override
        public void setState(int state) {
            if (state == InDoubtTransaction.COMMIT) {
                transaction.commit();
            } else {
                transaction.rollback();
            }
            store.commit();
            this.state = state;
        }

        @Override
        public String getState() {
            switch (state) {
            case IN_DOUBT:
                return "IN_DOUBT";
            case COMMIT:
                return "COMMIT";
            case ROLLBACK:
                return "ROLLBACK";
            default:
                throw DbException.throwInternalError("state=" + state);
            }
        }

        @Override
        public String getTransactionName() {
            return transaction.getName();
        }

    }

    @Override
    public boolean hasMap(org.lealone.storage.Database db, String name) {
        return getStore(db).getStore().hasMap(name);
    }

    @Override
    public boolean isInMemory(org.lealone.storage.Database db) {
        return getStore(db) == null;
    }

    @Override
    public void removeTable(org.lealone.storage.Table table) {
        getStore(((MVTable) table).getDatabase()).removeTable(((MVTable) table));
    }

    @Override
    public String nextTemporaryMapName(org.lealone.storage.Database db) {
        return getStore(db).nextTemporaryMapName();
    }

    @Override
    public <K, V> TransactionMap<K, V> openMap(SessionInterface session, String name, DataType keyType,
            DataType valueType) {
        return ((Session) session).getTransaction().openMap(name, keyType, valueType);
    }

    @Override
    public void backupTo(Database db0, String fileName) {
        org.lealone.db.Database db = (org.lealone.db.Database) db0;
        if (!db.isPersistent()) {
            throw DbException.get(ErrorCode.DATABASE_IS_NOT_PERSISTENT);
        }
        try {
            Store mvStore = getStore(db);
            if (mvStore != null) {
                mvStore.flush();
            }
            // 生成fileName表示的文件，如果已存在则覆盖原有的，也就是文件为空
            OutputStream zip = FileUtils.newOutputStream(fileName, false);
            ZipOutputStream out = new ZipOutputStream(zip);

            // synchronize on the database, to avoid concurrent temp file
            // creation / deletion / backup
            String base = FileUtils.getParent(db.getName());
            synchronized (db.getLobSyncObject()) {
                String prefix = db.getDatabasePath(); // 返回E:/H2/baseDir/mydb
                String dir = FileUtils.getParent(prefix); // 返回E:/H2/baseDir
                dir = getDir(dir); // 返回E:/H2/baseDir
                String name = db.getName(); // 返回E:/H2/baseDir/mydb
                name = FileUtils.getName(name); // 返回mydb(也就是只取简单文件名)
                ArrayList<String> fileList = getDatabaseFiles(dir, name, true);

                // 把".lob.db"和".mv.db"文件备份到fileName表示的文件中(是一个zip文件)
                for (String n : fileList) {
                    if (n.endsWith(Constants.SUFFIX_LOB_FILE)) { // 备份".lob.db"文件
                        backupFile(out, base, n);
                    } else if (n.endsWith(Constants.SUFFIX_MV_FILE) && mvStore != null) { // 备份".mv.db"文件
                        MVStore s = mvStore.getStore();
                        boolean before = s.getReuseSpace();
                        s.setReuseSpace(false);
                        try {
                            InputStream in = mvStore.getInputStream();
                            backupFile(out, base, n, in);
                        } finally {
                            s.setReuseSpace(before);
                        }
                    }
                }
            }
            out.close();
            zip.close();
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
    }

    private static void backupFile(ZipOutputStream out, String base, String fn) throws IOException {
        InputStream in = FileUtils.newInputStream(fn);
        backupFile(out, base, fn, in);
    }

    private static void backupFile(ZipOutputStream out, String base, String fn, InputStream in) throws IOException {
        String f = FileUtils.toRealPath(fn); // 返回E:/H2/baseDir/mydb.mv.db
        base = FileUtils.toRealPath(base); // 返回E:/H2/baseDir
        if (!f.startsWith(base)) {
            DbException.throwInternalError(f + " does not start with " + base);
        }
        f = f.substring(base.length()); // 返回/mydb.mv.db
        f = correctFileName(f); // 返回mydb.mv.db
        out.putNextEntry(new ZipEntry(f));
        IOUtils.copyAndCloseInput(in, out);
        out.closeEntry();
    }

    /**
     * Fix the file name, replacing backslash with slash.
     *
     * @param f the file name
     * @return the corrected file name
     */
    private static String correctFileName(String f) {
        f = f.replace('\\', '/');
        if (f.startsWith("/")) {
            f = f.substring(1);
        }
        return f;
    }

    /**
     * Normalize the directory name.
     *
     * @param dir the directory (null for the current directory)
     * @return the normalized directory name
     */
    private static String getDir(String dir) {
        if (dir == null || dir.equals("")) {
            return ".";
        }
        return FileUtils.toRealPath(dir);
    }

    /**
     * Get the list of database files.
     *
     * @param dir the directory (must be normalized)
     * @param db the database name (null for all databases)
     * @param all if true, files such as the lock, trace, and lob
     *            files are included. If false, only data, index, log,
     *            and lob files are returned
     * @return the list of files
     */
    private static ArrayList<String> getDatabaseFiles(String dir, String db, boolean all) {
        ArrayList<String> files = New.arrayList();
        // for Windows, File.getCanonicalPath("...b.") returns just "...b"
        String start = db == null ? null : (FileUtils.toRealPath(dir + "/" + db) + ".");
        for (String f : FileUtils.newDirectoryStream(dir)) {
            boolean ok = false;
            if (f.endsWith(Constants.SUFFIX_LOBS_DIRECTORY)) {
                if (start == null || f.startsWith(start)) {
                    files.addAll(getDatabaseFiles(f, null, all));
                    ok = true;
                }
            } else if (f.endsWith(Constants.SUFFIX_LOB_FILE)) {
                ok = true;
            } else if (f.endsWith(Constants.SUFFIX_MV_FILE)) {
                ok = true;
            } else if (all) {
                if (f.endsWith(Constants.SUFFIX_LOCK_FILE)) {
                    ok = true;
                } else if (f.endsWith(Constants.SUFFIX_TEMP_FILE)) {
                    ok = true;
                } else if (f.endsWith(Constants.SUFFIX_TRACE_FILE)) {
                    ok = true;
                }
            }
            if (ok) {
                if (db == null || f.startsWith(start)) {
                    String fileName = f;
                    files.add(fileName);
                }
            }
        }
        return files;
    }

    @Override
    public void flush(Database db) {
        getStore(db).flush();
    }

    @Override
    public void sync(Database db) {
        getStore(db).sync();
    }

    @Override
    public void initTransactions(Database db) {
        getStore(db).initTransactions();
    }

    @Override
    public void removeTemporaryMaps(Database db, BitField objectIds) {
        getStore(db).removeTemporaryMaps(objectIds);
    }

    @Override
    public void closeImmediately(Database db) {
        getStore(db).closeImmediately();
    }
}
