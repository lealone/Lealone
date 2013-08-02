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
package com.codefollower.lealone.mvstore.engine;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.codefollower.lealone.api.DatabaseEventListener;
import com.codefollower.lealone.command.dml.BackupCommand;
import com.codefollower.lealone.constant.Constants;
import com.codefollower.lealone.constant.ErrorCode;
import com.codefollower.lealone.constant.SysProperties;
import com.codefollower.lealone.dbobject.DbObject;
import com.codefollower.lealone.dbobject.SchemaObject;
import com.codefollower.lealone.dbobject.Sequence;
import com.codefollower.lealone.dbobject.User;
import com.codefollower.lealone.dbobject.index.IndexType;
import com.codefollower.lealone.dbobject.index.PersistentIndex;
import com.codefollower.lealone.dbobject.table.IndexColumn;
import com.codefollower.lealone.dbobject.table.MetaTable;
import com.codefollower.lealone.dbobject.table.TableBase;
import com.codefollower.lealone.engine.ConnectionInfo;
import com.codefollower.lealone.engine.Database;
import com.codefollower.lealone.engine.DatabaseEngine;
import com.codefollower.lealone.engine.InDoubtTransaction;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.message.Trace;
import com.codefollower.lealone.message.TraceSystem;
import com.codefollower.lealone.mvstore.dbobject.MVTableEngine;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.store.fs.FileUtils;
import com.codefollower.lealone.util.IOUtils;
import com.codefollower.lealone.util.MathUtils;
import com.codefollower.lealone.util.Utils;

public class MVDatabase extends Database {

    protected FileLock lock;
    protected int fileLockMethod;

    private volatile boolean checkpointRunning;
    private volatile int checkpointAllowed;
    private final Object reconnectSync = new Object();

    protected int reconnectCheckDelay;

    public MVDatabase(DatabaseEngine dbEngine, boolean persistent) {
        super(dbEngine, persistent);
    }

    @Override
    public String getTableEngineName() {
        return MVTableEngine.NAME;
    }

    @Override
    public void init(ConnectionInfo ci, String cipher) {
        this.reconnectCheckDelay = ci.getDbSettings().reconnectCheckDelay;
        String lockMethodName = ci.getProperty("FILE_LOCK", null);
        this.fileLockMethod = FileLock.getFileLockMethod(lockMethodName);
        super.init(ci, cipher);
    }

    @Override
    public synchronized MVSession createSession(User user) {
        if (exclusiveSession != null) {
            throw DbException.get(ErrorCode.DATABASE_IS_IN_EXCLUSIVE_MODE);
        }
        MVSession session = new MVSession(this, user, ++nextSessionId);
        userSessions.add(session);
        trace.info("connecting session #{0} to {1}", session.getId(), databaseName);
        if (delayedCloser != null) {
            delayedCloser.reset();
            delayedCloser = null;
        }
        return session;
    }

    @Override
    protected MVSession createSystemSession(User user, int id) {
        return new MVSession(this, user, id);
    }

    public void addPersistentMetaInfo(MetaTable mt, ArrayList<Row> rows) {
    }

    public void statisticsStart() {
    }

    public HashMap<String, Integer> statisticsEnd() {
        return new HashMap<String, Integer>(0);
    }

    protected synchronized void close(boolean fromShutdownHook) {
        if (closing) {
            return;
        }
        if (fileLockMethod == FileLock.LOCK_SERIALIZED && !reconnectChangePending) {
            // another connection may have written something - don't write
            try {
                closeOpenFilesAndUnlock(false);
            } catch (DbException e) {
                // ignore
            }
            traceSystem.close();
            dbEngine.closeDatabase(databaseName);
            return;
        }
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

    protected synchronized void closeFiles() {
    }

    protected synchronized void closeOpenFilesAndUnlock(boolean flush) {
        reconnectModified(false);

        closeFiles();
        if (persistent && lock == null && fileLockMethod != FileLock.LOCK_NO && fileLockMethod != FileLock.LOCK_FS) {
            // everything already closed (maybe in checkPowerOff)
            // don't delete temp files in this case because
            // the database could be open now (even from within another process)
            return;
        }
        if (persistent) {
            deleteOldTempFiles();
        }
        if (systemSession != null) {
            systemSession.close();
            systemSession = null;
        }
        if (lock != null) {
            if (fileLockMethod == FileLock.LOCK_SERIALIZED) {
                // wait before deleting the .lock file,
                // otherwise other connections can not detect that
                if (lock.load().containsKey("changePending")) {
                    try {
                        Thread.sleep((int) (reconnectCheckDelay * 1.1));
                    } catch (InterruptedException e) {
                        trace.error(e, "close");
                    }
                }
            }
            lock.unlock();
            lock = null;
        }
    }

    public void setLogMode(int log) {
        if (log < 0 || log > 2) {
            throw DbException.getInvalidValueException("LOG", log);
        }
    }

    /**
     * Flush all changes and open a new transaction log.
     */
    public void checkpoint() {
        getTempFileDeleter().deleteUnused();
    }

    /**
     * Synchronize the files with the file system. This method is called when
     * executing the SQL statement CHECKPOINT SYNC.
     */
    public synchronized void sync() {

    }

    public synchronized void setCacheSize(int kb) {
        if (starting) {
            int max = MathUtils.convertLongToInt(Utils.getMemoryMax()) / 2;
            kb = Math.min(kb, max);
        }
        cacheSize = kb;
    }

    /**
     * Get the list of in-doubt transactions.
     *
     * @return the list
     */
    public ArrayList<InDoubtTransaction> getInDoubtTransactions() {
        return null;
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

    }

    protected void checkPowerOffInternal() {

        if (lock != null) {
            stopServer();
            if (fileLockMethod != FileLock.LOCK_SERIALIZED) {
                // allow testing shutdown
                lock.unlock();
            }
            lock = null;
        }
    }

    public void setWriteDelay(int value) {
        writeDelay = value;
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
                        String pos = null;
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
            String pos = null;
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
            //backupPageStore(out, fn, getPageStore());
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

    //TODO
    public PersistentIndex createPersistentIndex(TableBase table, int indexId, String indexName, IndexColumn[] indexCols,
            IndexType indexType, boolean create, Session session) {
        return null;
    }

    /**
     * Check if the contents of the database was changed and therefore it is
     * required to re-connect. This method waits until pending changes are
     * completed. If a pending change takes too long (more than 2 seconds), the
     * pending change is broken (removed from the properties file).
     *
     * @return true if reconnecting is required
     */
    public boolean isReconnectNeeded() {
        if (fileLockMethod != FileLock.LOCK_SERIALIZED) {
            return false;
        }
        if (reconnectChangePending) {
            return false;
        }
        long now = System.currentTimeMillis();
        if (now < reconnectCheckNext) {
            return false;
        }
        reconnectCheckNext = now + reconnectCheckDelay;
        if (lock == null) {
            lock = new FileLock(traceSystem, databaseName + Constants.SUFFIX_LOCK_FILE, Constants.LOCK_SLEEP);
        }
        try {
            Properties prop = lock.load(), first = prop;
            while (true) {
                if (prop.equals(reconnectLastLock)) {
                    return false;
                }
                if (prop.getProperty("changePending", null) == null) {
                    break;
                }
                if (System.currentTimeMillis() > now + reconnectCheckDelay * 10) {
                    if (first.equals(prop)) {
                        // the writing process didn't update the file -
                        // it may have terminated
                        lock.setProperty("changePending", null);
                        lock.save();
                        break;
                    }
                }
                trace.debug("delay (change pending)");
                Thread.sleep(reconnectCheckDelay);
                prop = lock.load();
            }
            reconnectLastLock = prop;
        } catch (Exception e) {
            // DbException, InterruptedException
            trace.error(e, "readOnly {0}", readOnly);
            // ignore
        }
        return true;
    }

    /**
     * Flush all changes when using the serialized mode, and if there are
     * pending changes, and some time has passed. This switches to a new
     * transaction log and resets the change pending flag in
     * the .lock.db file.
     */
    public void checkpointIfRequired() {
        if (fileLockMethod != FileLock.LOCK_SERIALIZED || readOnly || !reconnectChangePending || closing) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now > reconnectCheckNext + reconnectCheckDelay) {
            if (SysProperties.CHECK && checkpointAllowed < 0) {
                DbException.throwInternalError();
            }
            synchronized (reconnectSync) {
                if (checkpointAllowed > 0) {
                    return;
                }
                checkpointRunning = true;
            }
            synchronized (this) {
                trace.debug("checkpoint start");
                flushSequences();
                checkpoint();
                reconnectModified(false);
                trace.debug("checkpoint end");
            }
            synchronized (reconnectSync) {
                checkpointRunning = false;
            }
        }
    }

    public boolean isFileLockSerialized() {
        return fileLockMethod == FileLock.LOCK_SERIALIZED;
    }

    private void flushSequences() {
        for (SchemaObject obj : getAllSchemaObjects(DbObject.SEQUENCE)) {
            Sequence sequence = (Sequence) obj;
            sequence.flushWithoutMargin();
        }
    }

    /**
     * This method is called before writing to the transaction log.
     *
     * @return true if the call was successful and writing is allowed,
     *          false if another connection was faster
     */
    public boolean beforeWriting() {
        if (fileLockMethod != FileLock.LOCK_SERIALIZED) {
            return true;
        }
        while (checkpointRunning) {
            try {
                Thread.sleep(10 + (int) (Math.random() * 10));
            } catch (Exception e) {
                // ignore InterruptedException
            }
        }
        synchronized (reconnectSync) {
            if (reconnectModified(true)) {
                checkpointAllowed++;
                if (SysProperties.CHECK && checkpointAllowed > 20) {
                    throw DbException.throwInternalError();
                }
                return true;
            }
        }
        // make sure the next call to isReconnectNeeded() returns true
        reconnectCheckNext = System.currentTimeMillis() - 1;
        reconnectLastLock = null;
        return false;
    }

    /**
     * This method is called after updates are finished.
     */
    public void afterWriting() {
        if (fileLockMethod != FileLock.LOCK_SERIALIZED) {
            return;
        }
        synchronized (reconnectSync) {
            checkpointAllowed--;
        }
        if (SysProperties.CHECK && checkpointAllowed < 0) {
            throw DbException.throwInternalError();
        }
    }

    public void checkWritingAllowed() {
        super.checkWritingAllowed();

        if (fileLockMethod == FileLock.LOCK_SERIALIZED) {
            if (!reconnectChangePending) {
                throw DbException.get(ErrorCode.DATABASE_IS_READ_ONLY);
            }
        }
    }
}
