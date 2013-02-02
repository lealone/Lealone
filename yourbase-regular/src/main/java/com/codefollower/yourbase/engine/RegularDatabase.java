/*
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.codefollower.yourbase.engine;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.codefollower.yourbase.api.DatabaseEventListener;
import com.codefollower.yourbase.command.dml.BackupCommand;
import com.codefollower.yourbase.constant.ErrorCode;
import com.codefollower.yourbase.constant.SysProperties;
import com.codefollower.yourbase.dbobject.index.IndexType;
import com.codefollower.yourbase.dbobject.index.PageBtreeIndex;
import com.codefollower.yourbase.dbobject.index.PersistentIndex;
import com.codefollower.yourbase.dbobject.table.IndexColumn;
import com.codefollower.yourbase.dbobject.table.MetaTable;
import com.codefollower.yourbase.dbobject.table.TableBase;
import com.codefollower.yourbase.message.DbException;
import com.codefollower.yourbase.message.Trace;
import com.codefollower.yourbase.message.TraceSystem;
import com.codefollower.yourbase.result.Row;
import com.codefollower.yourbase.store.FileLister;
import com.codefollower.yourbase.store.FileLock;
import com.codefollower.yourbase.store.PageStore;
import com.codefollower.yourbase.store.WriterThread;
import com.codefollower.yourbase.store.fs.FileUtils;
import com.codefollower.yourbase.tools.DeleteDbFiles;
import com.codefollower.yourbase.tools.Server;
import com.codefollower.yourbase.util.BitField;
import com.codefollower.yourbase.util.IOUtils;
import com.codefollower.yourbase.util.MathUtils;
import com.codefollower.yourbase.util.NetUtils;
import com.codefollower.yourbase.util.Utils;

public class RegularDatabase extends Database {
    protected PageStore pageStore;
    protected int pageSize;
    protected WriterThread writer;
    protected Server server;

    @Override
    public void init(ConnectionInfo ci, String cipher) {
        this.pageSize = ci.getProperty("PAGE_SIZE", Constants.DEFAULT_PAGE_SIZE);
        super.init(ci, cipher);
    }

    public void addPersistentMetaInfo(MetaTable mt, ArrayList<Row> rows) {
        mt.add(rows, "info.FILE_WRITE_TOTAL", "" + pageStore.getWriteCountTotal());
        mt.add(rows, "info.FILE_WRITE", "" + pageStore.getWriteCount());
        mt.add(rows, "info.FILE_READ", "" + pageStore.getReadCount());
        mt.add(rows, "info.PAGE_COUNT", "" + pageStore.getPageCount());
        mt.add(rows, "info.PAGE_SIZE", "" + pageStore.getPageSize());
        mt.add(rows, "info.CACHE_MAX_SIZE", "" + pageStore.getCache().getMaxMemory());
        mt.add(rows, "info.CACHE_SIZE", "" + pageStore.getCache().getMemory());
    }

    public void statisticsStart() {
        pageStore.statisticsStart();
    }

    public HashMap<String, Integer> statisticsEnd() {
        return pageStore.statisticsEnd();
    }

    protected synchronized void close(boolean fromShutdownHook) {
        super.close(fromShutdownHook);
        if (deleteFilesOnDisconnect && persistent) {
            deleteFilesOnDisconnect = false;
            try {
                String directory = FileUtils.getParent(databaseName);
                String name = FileUtils.getName(databaseName);
                DeleteDbFiles.execute(directory, name, true);
            } catch (Exception e) {
                // ignore (the trace is closed already)
            }
        }
    }

    public PageStore getPageStore() {
        if (pageStore == null) {
            pageStore = new PageStore(this, databaseName + Constants.SUFFIX_PAGE_FILE, accessModeData, cacheSize);
            if (pageSize != Constants.DEFAULT_PAGE_SIZE) {
                pageStore.setPageSize(pageSize);
            }
            if (!readOnly && fileLockMethod == FileLock.LOCK_FS) {
                pageStore.setLockFile(true);
            }
            pageStore.setLogMode(logMode);
            pageStore.open();
        }
        return pageStore;
    }

    protected void startServer(String key) {
        try {
            server = Server.createTcpServer("-tcpPort", Integer.toString(autoServerPort), "-tcpAllowOthers", "-tcpDaemon",
                    "-key", key, databaseName);
            server.start();
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
        String address = NetUtils.getLocalAddress() + ":" + server.getPort();
        lock.setProperty("server", address);
        lock.save();
    }

    protected void stopServer() {
        if (server != null) {
            Server s = server;
            // avoid calling stop recursively
            // because stopping the server will
            // try to close the database as well
            server = null;
            s.stop();
        }
    }

    public long getFileWriteCountTotal() {
        return pageStore.getWriteCountTotal();
    }

    public long getFileWriteCount() {
        return pageStore.getWriteCount();
    }

    public long getFileReadCount() {
        return pageStore.getReadCount();
    }

    public long getFileSize() {
        return pageStore.getPageCount() * pageStore.getPageSize() / 1024;
    }

    public int getCacheSizeMax() {
        return pageStore.getCache().getMaxMemory();
    }

    public void setCacheSizeMax(int kb) {
        pageStore.getCache().setMaxMemory(kb);
    }

    public int getCacheSize() {
        return pageStore.getCache().getMemory();
    }

    protected synchronized void closeFiles() {
        try {
            if (pageStore != null) {
                pageStore.close();
                pageStore = null;
            }
        } catch (DbException e) {
            trace.error(e, "close");
        }
    }

    protected synchronized void closeOpenFilesAndUnlockInternal(boolean flush) {
        stopWriter();
        if (pageStore != null) {
            if (flush) {
                try {
                    pageStore.checkpoint();
                    if (!readOnly) {
                        lockMeta(pageStore.getSystemSession());
                        pageStore.compact(compactMode);
                    }
                } catch (DbException e) {
                    if (SysProperties.CHECK2) {
                        int code = e.getErrorCode();
                        if (code != ErrorCode.DATABASE_IS_CLOSED && code != ErrorCode.LOCK_TIMEOUT_1
                                && code != ErrorCode.IO_EXCEPTION_2) {
                            e.printStackTrace();
                        }
                    }
                    trace.error(e, "close");
                } catch (Throwable t) {
                    if (SysProperties.CHECK2) {
                        t.printStackTrace();
                    }
                    trace.error(t, "close");
                }
            }
        }
    }

    public void setLogMode(int log) {
        if (log < 0 || log > 2) {
            throw DbException.getInvalidValueException("LOG", log);
        }
        if (pageStore != null) {
            if (log != PageStore.LOG_MODE_SYNC || pageStore.getLogMode() != PageStore.LOG_MODE_SYNC) {
                // write the log mode in the trace file when enabling or
                // disabling a dangerous mode
                trace.error(null, "log {0}", log);
            }
            this.logMode = log;
            pageStore.setLogMode(log);
        }
    }

    public int getLogMode() {
        if (pageStore != null) {
            return pageStore.getLogMode();
        }
        return PageStore.LOG_MODE_OFF;
    }

    /**
     * Flush all changes and open a new transaction log.
     */
    public void checkpoint() {
        if (persistent) {
            synchronized (this) {
                if (pageStore != null) {
                    pageStore.checkpoint();
                }
            }
        }
        getTempFileDeleter().deleteUnused();
    }

    public synchronized void setMaxLogSize(long value) {
        if (pageStore != null) {
            pageStore.setMaxLogSize(value);
        }
    }

    /**
     * Synchronize the files with the file system. This method is called when
     * executing the SQL statement CHECKPOINT SYNC.
     */
    public synchronized void sync() {
        if (readOnly || pageStore == null) {
            return;
        }
        pageStore.sync();
    }

    public synchronized void setCacheSize(int kb) {
        if (starting) {
            int max = MathUtils.convertLongToInt(Utils.getMemoryMax()) / 2;
            kb = Math.min(kb, max);
        }
        cacheSize = kb;
        if (pageStore != null) {
            pageStore.getCache().setMaxMemory(kb);
        }
    }

    /**
     * Get the list of in-doubt transactions.
     *
     * @return the list
     */
    public ArrayList<InDoubtTransaction> getInDoubtTransactions() {
        return pageStore == null ? null : pageStore.getInDoubtTransactions();
    }

    /**
     * Prepare a transaction.
     *
     * @param session the session
     * @param transaction the name of the transaction
     */
    synchronized void prepareCommit(Session session, String transaction) {
        if (readOnly) {
            return;
        }
        pageStore.prepareCommit(session, transaction);
    }

    /**
     * Commit the current transaction of the given session.
     *
     * @param session the session
     */
    synchronized void commit(Session session) {
        if (readOnly) {
            return;
        }
        if (pageStore != null) {
            pageStore.commit(session);
        }
        session.setAllCommitted();
    }

    /**
     * Flush all pending changes to the transaction log.
     */
    public synchronized void flush() {
        if (readOnly || pageStore == null) {
            return;
        }
        pageStore.flushLog();
    }

    public void setEventListener(DatabaseEventListener eventListener) {
        this.eventListener = eventListener;
    }

    protected void preOpen(int traceLevelFile, int traceLevelSystemOut) {

        String dataFileName = databaseName + ".data.db";
        boolean existsData = FileUtils.exists(dataFileName);
        String pageFileName = databaseName + Constants.SUFFIX_PAGE_FILE;
        boolean existsPage = FileUtils.exists(pageFileName);
        if (existsData && !existsPage) {
            throw DbException.get(ErrorCode.FILE_VERSION_ERROR_1, "Old database: " + dataFileName
                    + " - please convert the database to a SQL script and re-create it.");
        }
        if (existsPage && !FileUtils.canWrite(pageFileName)) {
            readOnly = true;
        }
        if (readOnly) {
            traceSystem = new TraceSystem(null);
        } else {
            traceSystem = new TraceSystem(databaseName + Constants.SUFFIX_TRACE_FILE);
        }
        traceSystem.setLevelFile(traceLevelFile);
        traceSystem.setLevelSystemOut(traceLevelSystemOut);
        trace = traceSystem.getTrace(Trace.DATABASE);
        trace.info("opening {0} (build {1})", databaseName, Constants.BUILD_ID);
        if (autoServerMode) {
            if (readOnly || fileLockMethod == FileLock.LOCK_NO || fileLockMethod == FileLock.LOCK_SERIALIZED
                    || fileLockMethod == FileLock.LOCK_FS || !persistent) {
                throw DbException.getUnsupportedException("autoServerMode && (readOnly || fileLockMethod == NO"
                        + " || fileLockMethod == SERIALIZED || inMemory)");
            }
        }
        String lockFileName = databaseName + Constants.SUFFIX_LOCK_FILE;
        if (readOnly) {
            if (FileUtils.exists(lockFileName)) {
                throw DbException.get(ErrorCode.DATABASE_ALREADY_OPEN_1, "Lock file exists: " + lockFileName);
            }
        }
        if (!readOnly && fileLockMethod != FileLock.LOCK_NO) {
            if (fileLockMethod != FileLock.LOCK_FS) {
                lock = new FileLock(traceSystem, lockFileName, Constants.LOCK_SLEEP);
                lock.lock(fileLockMethod);
                if (autoServerMode) {
                    startServer(lock.getUniqueId());
                }
            }
        }
        if (SysProperties.MODIFY_ON_WRITE) {
            while (isReconnectNeeded()) {
                // wait until others stopped writing
            }
        } else {
            while (isReconnectNeeded() && !beforeWriting()) {
                // wait until others stopped writing and
                // until we can write (the file is not yet open -
                // no need to re-connect)
            }
        }
        deleteOldTempFiles();
        starting = true;
        if (SysProperties.MODIFY_ON_WRITE) {
            try {
                getPageStore();
            } catch (DbException e) {
                if (e.getErrorCode() != ErrorCode.DATABASE_IS_READ_ONLY) {
                    throw e;
                }
                pageStore = null;
                while (!beforeWriting()) {
                    // wait until others stopped writing and
                    // until we can write (the file is not yet open -
                    // no need to re-connect)
                }
                getPageStore();
            }
        } else {
            getPageStore();
        }
        starting = false;
        writer = WriterThread.create(this, writeDelay);
    }

    protected void postOpen() {
        // mark all ids used in the page store
        if (pageStore != null) {
            BitField f = pageStore.getObjectIds();
            for (int i = 0, len = f.length(); i < len; i++) {
                if (f.get(i) && !objectIds.get(i)) {
                    trace.info("unused object id: " + i);
                    objectIds.set(i);
                }
            }
        }
    }

    @Override
    protected void openMetaTable(boolean create) {
        if (pageStore != null) {
            create = pageStore.isNew();
        }
        super.openMetaTable(create);
    }

    private void stopWriter() {
        if (writer != null) {
            writer.stopThread();
            writer = null;
        }
    }

    protected void checkPowerOffInternal() {
        stopWriter();
        if (pageStore != null) {
            try {
                pageStore.close();
            } catch (DbException e) {
                // ignore
            }
            pageStore = null;
        }
    }

    public void setWriteDelay(int value) {
        writeDelay = value;
        if (writer != null) {
            writer.setWriteDelay(value);
            // TODO check if MIN_WRITE_DELAY is a good value
            flushOnEachCommit = writeDelay < Constants.MIN_WRITE_DELAY;
        }
    }

    /**
     * Called after the database has been opened and initialized. This method
     * notifies the event listener if one has been set.
     */
    void opened() {
        if (eventListener != null) {
            eventListener.opened();
        }
        if (writer != null) {
            writer.startThread();
        }
    }

    /**
     * Set or reset the pending change flag in the .lock.db file.
     *
     * @param pending the new value of the flag
     * @return true if the call was successful,
     *          false if another connection was faster
     */
    protected synchronized boolean reconnectModified(boolean pending) {
        if (readOnly || lock == null || fileLockMethod != FileLock.LOCK_SERIALIZED) {
            return true;
        }
        try {
            if (pending == reconnectChangePending) {
                long now = System.currentTimeMillis();
                if (now > reconnectCheckNext) {
                    if (pending) {
                        String pos = pageStore == null ? null : "" + pageStore.getWriteCountTotal();
                        lock.setProperty("logPos", pos);
                        lock.save();
                    }
                    reconnectCheckNext = now + reconnectCheckDelay;
                }
                return true;
            }
            Properties old = lock.load();
            if (pending) {
                if (old.getProperty("changePending") != null) {
                    return false;
                }
                trace.debug("wait before writing");
                Thread.sleep((int) (reconnectCheckDelay * 1.1));
                Properties now = lock.load();
                if (!now.equals(old)) {
                    // somebody else was faster
                    return false;
                }
            }
            String pos = pageStore == null ? null : "" + pageStore.getWriteCountTotal();
            lock.setProperty("logPos", pos);
            if (pending) {
                lock.setProperty("changePending", "true-" + Math.random());
            } else {
                lock.setProperty("changePending", null);
            }
            // ensure that the writer thread will
            // not reset the flag before we are done
            reconnectCheckNext = System.currentTimeMillis() + 2 * reconnectCheckDelay;
            old = lock.save();
            if (pending) {
                trace.debug("wait before writing again");
                Thread.sleep((int) (reconnectCheckDelay * 1.1));
                Properties now = lock.load();
                if (!now.equals(old)) {
                    // somebody else was faster
                    return false;
                }
            } else {
                Thread.sleep(1);
            }
            reconnectLastLock = old;
            reconnectChangePending = pending;
            reconnectCheckNext = System.currentTimeMillis() + reconnectCheckDelay;
            return true;
        } catch (Exception e) {
            trace.error(e, "pending {0}", pending);
            return false;
        }
    }

    public void backupTo(String fileName) {

        if (!isPersistent()) {
            throw DbException.get(ErrorCode.DATABASE_IS_NOT_PERSISTENT);
        }
        try {
            //MVTableEngine.flush(db); //TODO 解决引用MVTableEngine类的问题
            String name = getName();
            name = FileUtils.getName(name);
            OutputStream zip = FileUtils.newOutputStream(fileName, false);
            ZipOutputStream out = new ZipOutputStream(zip);
            flush();
            String fn = getName() + Constants.SUFFIX_PAGE_FILE;
            backupPageStore(out, fn, getPageStore());
            // synchronize on the database, to avoid concurrent temp file
            // creation / deletion / backup
            String base = FileUtils.getParent(fn);
            synchronized (getLobSyncObject()) {
                String prefix = getDatabasePath();
                String dir = FileUtils.getParent(prefix);
                dir = FileLister.getDir(dir);
                ArrayList<String> fileList = FileLister.getDatabaseFiles(dir, name, true);
                for (String n : fileList) {
                    if (n.endsWith(Constants.SUFFIX_LOB_FILE)) {
                        backupFile(out, base, n);
                    }
                    if (n.endsWith(Constants.SUFFIX_MV_FILE)) {
                        backupFile(out, base, n);
                    }
                }
            }
            out.close();
            zip.close();
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
    }

    private void backupPageStore(ZipOutputStream out, String fileName, PageStore store) throws IOException {
        fileName = FileUtils.getName(fileName);
        out.putNextEntry(new ZipEntry(fileName));
        int pos = 0;
        try {
            store.setBackup(true);
            while (true) {
                pos = store.copyDirect(pos, out);
                if (pos < 0) {
                    break;
                }
                int max = store.getPageCount();
                setProgress(DatabaseEventListener.STATE_BACKUP_FILE, fileName, pos, max);
            }
        } finally {
            store.setBackup(false);
        }
        out.closeEntry();
    }

    private static void backupFile(ZipOutputStream out, String base, String fn) throws IOException {
        String f = FileUtils.toRealPath(fn);
        base = FileUtils.toRealPath(base);
        if (!f.startsWith(base)) {
            DbException.throwInternalError(f + " does not start with " + base);
        }
        f = f.substring(base.length());
        f = BackupCommand.correctFileName(f);
        out.putNextEntry(new ZipEntry(f));
        InputStream in = FileUtils.newInputStream(fn);
        IOUtils.copyAndCloseInput(in, out);
        out.closeEntry();
    }

    public PersistentIndex createPersistentIndex(TableBase table, int indexId, String indexName, IndexColumn[] indexCols,
            IndexType indexType, boolean create, Session session) {
        return new PageBtreeIndex(table, indexId, indexName, indexCols, indexType, create, session);
    }
}
