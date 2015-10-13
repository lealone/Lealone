/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.storage;

import java.nio.file.FileStore;
import java.util.HashMap;

import org.lealone.common.util.DataUtils;
import org.lealone.common.util.New;

/**
 * A storage builder.
 * 
 * @author H2 Group
 * @author zhh
 */
public abstract class StorageBuilder {

    protected final HashMap<String, Object> config = New.hashMap();

    public abstract Storage openStorage();

    private StorageBuilder set(String key, Object value) {
        config.put(key, value);
        return this;
    }

    /**
     * Disable auto-commit, by setting the auto-commit delay and auto-commit
     * buffer size to 0.
     * 
     * @return this
     */
    public StorageBuilder autoCommitDisabled() {
        // we have a separate config option so that
        // no thread is started if the write delay is 0
        // (if we only had a setter in the AOStore,
        // the thread would need to be started in any case)
        set("autoCommitBufferSize", 0);
        return set("autoCommitDelay", 0);
    }

    /**
     * Set the size of the write buffer, in KB disk space (for file-based
     * stores). Unless auto-commit is disabled, changes are automatically
     * saved if there are more than this amount of changes.
     * <p>
     * The default is 1024 KB.
     * <p>
     * When the value is set to 0 or lower, data is not automatically
     * stored.
     * 
     * @param kb the write buffer size, in kilobytes
     * @return this
     */
    public StorageBuilder autoCommitBufferSize(int kb) {
        return set("autoCommitBufferSize", kb);
    }

    /**
     * Set the auto-compact target fill rate. If the average fill rate (the
     * percentage of the storage space that contains active data) of the
     * chunks is lower, then the chunks with a low fill rate are re-written.
     * Also, if the percentage of empty space between chunks is higher than
     * this value, then chunks at the end of the file are moved. Compaction
     * stops if the target fill rate is reached.
     * <p>
     * The default value is 50 (50%). The value 0 disables auto-compacting.
     * <p>
     * 
     * @param percent the target fill rate
     * @return this
     */
    public StorageBuilder autoCompactFillRate(int percent) {
        return set("autoCompactFillRate", percent);
    }

    /**
     * Use the following storage name. If the file does not exist, it is
     * automatically created. The parent directory already must exist.
     * 
     * @param storageName the storage name
     * @return this
     */
    public StorageBuilder storageName(String storageName) {
        return set("storageName", storageName);
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
        return set("encryptionKey", password);
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
        return set("readOnly", 1);
    }

    /**
     * Open the file in memory mode, meaning that changes are not persisted. 
     * 
     * @return this
     */
    public StorageBuilder inMemory() {
        return set("inMemory", 1);
    }

    /**
     * Set the read cache size in MB. The default is 16 MB.
     * 
     * @param mb the cache size in megabytes
     * @return this
     */
    public StorageBuilder cacheSize(int mb) {
        return set("cacheSize", mb);
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
        return set("compress", 1);
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
        return set("compress", 2);
    }

    /**
     * Set the amount of memory a page should contain at most, in bytes,
     * before it is split. The default is 16 KB for persistent stores and 4
     * KB for in-memory stores. This is not a limit in the page size, as
     * pages with one entry can get larger. It is just the point where pages
     * that contain more than one entry are split.
     * 
     * @param pageSplitSize the page size
     * @return this
     */
    public StorageBuilder pageSplitSize(int pageSplitSize) {
        return set("pageSplitSize", pageSplitSize);
    }

    /**
     * Set the listener to be used for exceptions that occur when writing in
     * the background thread.
     * 
     * @param exceptionHandler the handler
     * @return this
     */
    public StorageBuilder backgroundExceptionHandler(Thread.UncaughtExceptionHandler exceptionHandler) {
        return set("backgroundExceptionHandler", exceptionHandler);
    }

    /**
     * Use the provided file storage instead of the default one.
     * <p>
     * File storages passed in this way need to be open. They are not closed
     * when closing the storage.
     * <p>
     * Please note that any kind of storage (including an off-heap storage) is
     * considered a "persistence", while an "in-memory storage" means objects
     * are not persisted and fully kept in the JVM heap.
     * 
     * @param fileStorage the file storage
     * @return this
     */
    public StorageBuilder fileStorage(FileStore fileStorage) {
        return set("fileStorage", fileStorage);
    }

    /**
     * How long to retain old, persisted chunks, in milliseconds. Chunks that
     * are older may be overwritten once they contain no live data.
     * <p>
     * The default value is 45000 (45 seconds) when using the default file
     * store. It is assumed that a file system and hard disk will flush all
     * write buffers within this time. Using a lower value might be dangerous,
     * unless the file system and hard disk flush the buffers earlier. To
     * manually flush the buffers, use
     * <code>MVStore.getFile().force(true)</code>, however please note that
     * according to various tests this does not always work as expected
     * depending on the operating system and hardware.
     * <p>
     * The retention time needs to be long enough to allow reading old chunks
     * while traversing over the entries of a map.
     * <p>
     * This setting is not persisted.
     * 
     * @param ms how many milliseconds to retain old chunks (0 to overwrite them
     *            as early as possible)
     */
    public StorageBuilder retentionTime(int ms) {
        return set("retentionTime", ms);
    }

    /**
     * How many versions to retain for in-memory stores. If not set, 5 old
     * versions are retained.
     * 
     * @param count the number of versions to keep
     */
    public StorageBuilder versionsToKeep(int count) {
        return set("versionsToKeep", count);
    }

    /**
     * Whether empty space in the file should be re-used. If enabled, old data
     * is overwritten (default). If disabled, writes are appended at the end of
     * the file.
     * <p>
     * This setting is specially useful for online backup. To create an online
     * backup, disable this setting, then copy the file (starting at the
     * beginning of the file). In this case, concurrent backup and write
     * operations are possible (obviously the backup process needs to be faster
     * than the write operations).
     */
    public StorageBuilder reuseSpace() {
        return set("reuseSpace", 1);
    }

    @Override
    public String toString() {
        return DataUtils.appendMap(new StringBuilder(), config).toString();
    }

}
