/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.storage;

import java.util.HashMap;

import com.lealone.common.util.DataUtils;
import com.lealone.db.DbSetting;
import com.lealone.db.scheduler.SchedulerFactory;

/**
 * A storage builder.
 * 
 * @author H2 Group
 * @author zhh
 */
public abstract class StorageBuilder {

    protected final HashMap<String, Object> config = new HashMap<>();

    /**
     * Open the storage.
     * 
     * @return the opened storage
     */
    public abstract Storage openStorage();

    protected StorageBuilder set(String key, Object value) {
        config.put(key, value);
        return this;
    }

    /**
     * Use the following storage path. If the file does not exist, it is
     * automatically created. The parent directory already must exist.
     * 
     * @param storagePath the storage path
     * @return this
     */
    public StorageBuilder storagePath(String storagePath) {
        return set(StorageSetting.STORAGE_PATH.name(), storagePath);
    }

    /**
     * Encrypt / decrypt the file using the given password. This method has
     * no effect for in-memory stores. The password is passed as a char
     * array so that it can be cleared as soon as possible. Please note
     * there is still a small risk that password stays in memory (due to
     * Java garbage collection). Also, the hashed encryption key is kept in
     * memory as long as the file is open.
     * 
     * @param password the password
     * @return this
     */
    public StorageBuilder encryptionKey(char[] password) {
        return set(StorageSetting.ENCRYPTION_KEY.name(), password);
    }

    /**
     * Open the file in read-only mode. In this case, a shared lock will be
     * acquired to ensure the file is not concurrently opened in write mode.
     * <p>
     * If this option is not used, the file is locked exclusively.
     * <p>
     * Please note a store may only be opened once in every JVM (no matter
     * whether it is opened in read-only or read-write mode), because each
     * file may be locked only once in a process.
     * 
     * @return this
     */
    public StorageBuilder readOnly() {
        return set(DbSetting.READ_ONLY.name(), 1);
    }

    /**
     * Open the file in memory mode, meaning that changes are not persisted. 
     * 
     * @return this
     */
    public StorageBuilder inMemory() {
        return set(StorageSetting.IN_MEMORY.name(), 1);
    }

    /**
     * Set the read cache size in MB. The default is 16 MB.
     * 
     * @param mb the cache size in megabytes
     * @return this
     */
    public StorageBuilder cacheSize(int mb) {
        return set(DbSetting.CACHE_SIZE.name(), mb * 1024 * 1024);
    }

    /**
     * Compress data before writing using the LZF algorithm. This will save
     * about 50% of the disk space, but will slow down read and write
     * operations slightly.
     * <p>
     * This setting only affects writes; it is not necessary to enable
     * compression when reading, even if compression was enabled when
     * writing.
     * 
     * @return this
     */
    public StorageBuilder compress() {
        return set(DbSetting.COMPRESS.name(), 1);
    }

    /**
     * Compress data before writing using the Deflate algorithm. This will
     * save more disk space, but will slow down read and write operations
     * quite a bit.
     * <p>
     * This setting only affects writes; it is not necessary to enable
     * compression when reading, even if compression was enabled when
     * writing.
     * 
     * @return this
     */
    public StorageBuilder compressHigh() {
        return set(DbSetting.COMPRESS.name(), 2);
    }

    /**
     * Set the amount of memory a page should contain at most, in bytes,
     * before it is split. The default is 16 KB for persistent stores and 4
     * KB for in-memory stores. This is not a limit in the page size, as
     * pages with one entry can get larger. It is just the point where pages
     * that contain more than one entry are split.
     * 
     * @param pageSize the page size
     * @return this
     */
    public StorageBuilder pageSize(int pageSize) {
        return set(DbSetting.PAGE_SIZE.name(), pageSize);
    }

    public StorageBuilder minFillRate(int minFillRate) {
        return set(StorageSetting.MIN_FILL_RATE.name(), minFillRate);
    }

    public StorageBuilder schedulerFactory(SchedulerFactory schedulerFactory) {
        return set(StorageSetting.SCHEDULER_FACTORY.name(), schedulerFactory);
    }

    @Override
    public String toString() {
        return DataUtils.appendMap(new StringBuilder(), config).toString();
    }
}
