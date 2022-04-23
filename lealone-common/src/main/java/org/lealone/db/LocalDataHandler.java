/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.MathUtils;
import org.lealone.common.util.TempFileDeleter;
import org.lealone.db.api.ErrorCode;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.fs.FileUtils;
import org.lealone.storage.lob.LobLocalStorage;
import org.lealone.storage.lob.LobLocalStorage.LobReader;
import org.lealone.storage.lob.LobStorage;

public class LocalDataHandler implements DataHandler {

    private final String cipher;
    private final byte[] fileEncryptionKey;
    private final Object lobSyncObject = new Object();
    private LobReader lobReader;
    private LobStorage lobStorage;

    public LocalDataHandler() {
        this(null);
    }

    public LocalDataHandler(String cipher) {
        this.cipher = cipher;
        fileEncryptionKey = cipher == null ? null : MathUtils.secureRandomBytes(32);
    }

    public void setLobReader(LobReader lobReader) {
        this.lobReader = lobReader;
    }

    @Override
    public String getDatabasePath() {
        return "";
    }

    @Override
    public FileStorage openFile(String name, String mode, boolean mustExist) {
        if (mustExist && !FileUtils.exists(name)) {
            throw DbException.get(ErrorCode.FILE_NOT_FOUND_1, name);
        }
        FileStorage fileStorage = FileStorage.open(this, name, mode, cipher, fileEncryptionKey, 0);
        fileStorage.setCheckedWriting(false);
        try {
            fileStorage.init();
        } catch (DbException e) {
            fileStorage.closeSilently();
            throw e;
        }
        return fileStorage;
    }

    @Override
    public TempFileDeleter getTempFileDeleter() {
        return TempFileDeleter.getInstance();
    }

    @Override
    public void checkPowerOff() {
        // ok
    }

    @Override
    public void checkWritingAllowed() {
        // ok
    }

    @Override
    public int getMaxLengthInplaceLob() {
        return SysProperties.LOB_CLIENT_MAX_SIZE_MEMORY;
    }

    @Override
    public String getLobCompressionAlgorithm(int type) {
        return null;
    }

    @Override
    public Object getLobSyncObject() {
        return lobSyncObject;
    }

    @Override
    public LobStorage getLobStorage() {
        if (lobStorage == null) {
            lobStorage = new LobLocalStorage(this, lobReader);
        }
        return lobStorage;
    }
}
