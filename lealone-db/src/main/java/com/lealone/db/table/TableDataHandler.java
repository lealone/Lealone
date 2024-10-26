/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.table;

import java.io.File;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.TempFileDeleter;
import com.lealone.db.DataHandler;
import com.lealone.db.Database;
import com.lealone.storage.Storage;
import com.lealone.storage.StorageBuilder;
import com.lealone.storage.StorageEngine;
import com.lealone.storage.fs.FileStorage;
import com.lealone.storage.lob.LobStorage;
import com.lealone.transaction.TransactionEngine.GcTask;

public class TableDataHandler implements DataHandler {

    private final Database db;
    private final LobStorage lobStorage;

    public TableDataHandler(StandardTable table, String name) {
        db = table.getDatabase();
        String storagePath = db.getStoragePath() + File.separator + name + File.separator + "lob";
        StorageEngine storageEngine = table.getStorageEngine();
        StorageBuilder storageBuilder = db.getStorageBuilder(storageEngine, storagePath);
        Storage storage = storageBuilder.openStorage();
        lobStorage = storageEngine.getLobStorage(this, storage);
        db.getTransactionEngine().addGcTask((GcTask) lobStorage);
    }

    @Override
    public String getDatabasePath() {
        return db.getDatabasePath();
    }

    @Override
    public FileStorage openFile(String name, String mode, boolean mustExist) {
        return db.openFile(name, mode, mustExist);
    }

    @Override
    public TempFileDeleter getTempFileDeleter() {
        return db.getTempFileDeleter();
    }

    @Override
    public void checkPowerOff() throws DbException {
        db.checkPowerOff();
    }

    @Override
    public void checkWritingAllowed() throws DbException {
        db.checkWritingAllowed();
    }

    @Override
    public int getMaxLengthInplaceLob() {
        return db.getMaxLengthInplaceLob();
    }

    @Override
    public String getLobCompressionAlgorithm(int type) {
        return db.getLobCompressionAlgorithm(type);
    }

    @Override
    public LobStorage getLobStorage() {
        return lobStorage;
    }

    @Override
    public boolean isTableLobStorage() {
        return true;
    }
}
