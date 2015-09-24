/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.lealone.aostore;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.lealone.aostore.btree.BTreeMap;
import org.lealone.aostore.btree.BTreeStore;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.New;
import org.lealone.storage.fs.FilePath;
import org.lealone.storage.fs.FileUtils;

// adaptive optimization store
public class AOStore {
    /**
     * The file name suffix of a AOStore file.
     */
    public static final String SUFFIX_AO_FILE = ".db"; // ".ao.db";

    /**
     * The file name suffix of a new AOStore file, used when compacting a store.
     */
    public static final String SUFFIX_AO_STORE_NEW_FILE = ".newFile";

    /**
     * The file name suffix of a temporary AOStore file, used when compacting a
     * store.
     */
    public static final String SUFFIX_AO_STORE_TEMP_FILE = ".tempFile";

    private final HashMap<String, Object> config;
    private final HashMap<String, BTreeMap<?, ?>> maps = new HashMap<String, BTreeMap<?, ?>>();

    private int lastMapId;

    AOStore(HashMap<String, Object> config) {
        this.config = config;
        Object storeName = config.get("storeName");
        if (storeName != null) {
            String sn = storeName.toString();
            if (!FileUtils.exists(sn))
                FileUtils.createDirectory(sn);

            FilePath dir = FilePath.get(sn);
            for (FilePath file : dir.newDirectoryStream()) {
                String name = file.getName();
                maps.put(name.substring(0, name.length() - SUFFIX_AO_FILE.length()), null);
            }
        }
    }

    public HashMap<String, Object> getConfig() {
        return config;
    }

    @SuppressWarnings("unchecked")
    public synchronized <M extends BTreeMap<K, V>, K, V> M openMap(String name, BTreeMap.MapBuilder<M, K, V> builder) {
        M map = (M) maps.get(name);
        if (map == null) {
            HashMap<String, Object> c = New.hashMap();
            int id = ++lastMapId;
            c.put("id", id);
            c.put("createVersion", 0L);
            map = builder.create();

            BTreeStore store = new BTreeStore(name, config);
            map.init(store, c);

            if (!maps.containsKey(name)) // map不存在时标数据改变
                store.markMetaChanged();
            maps.put(name, map);
        }

        return map;
    }

    public synchronized void close() {
        for (BTreeMap<?, ?> map : maps.values())
            map.close();
    }

    public synchronized void commit() {
        for (BTreeMap<?, ?> map : maps.values())
            map.commit();
    }

    public synchronized Set<String> getMapNames() {
        return new HashSet<String>(maps.keySet());
    }

    /**
     * Open a store in exclusive mode. For a file-based store, the parent
     * directory must already exist.
     * 
     * @param storeName the store name (null for in-memory)
     * @return the store
     */
    public static AOStore open(String storeName) {
        HashMap<String, Object> config = New.hashMap();
        config.put("storeName", storeName);
        return new AOStore(config);
    }

    /**
     * A builder for an AOStore.
     */
    public static class Builder {

        private final HashMap<String, Object> config = New.hashMap();

        private Builder set(String key, Object value) {
            config.put(key, value);
            return this;
        }

        /**
         * Disable auto-commit, by setting the auto-commit delay and auto-commit
         * buffer size to 0.
         * 
         * @return this
         */
        public Builder autoCommitDisabled() {
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
        public Builder autoCommitBufferSize(int kb) {
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
        public Builder autoCompactFillRate(int percent) {
            return set("autoCompactFillRate", percent);
        }

        /**
         * Use the following store name. If the file does not exist, it is
         * automatically created. The parent directory already must exist.
         * 
         * @param storeName the store name
         * @return this
         */
        public Builder storeName(String storeName) {
            return set("storeName", storeName);
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
        public Builder encryptionKey(char[] password) {
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
        public Builder readOnly() {
            return set("readOnly", 1);
        }

        /**
         * Set the read cache size in MB. The default is 16 MB.
         * 
         * @param mb the cache size in megabytes
         * @return this
         */
        public Builder cacheSize(int mb) {
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
        public Builder compress() {
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
        public Builder compressHigh() {
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
        public Builder pageSplitSize(int pageSplitSize) {
            return set("pageSplitSize", pageSplitSize);
        }

        /**
         * Set the listener to be used for exceptions that occur when writing in
         * the background thread.
         * 
         * @param exceptionHandler the handler
         * @return this
         */
        public Builder backgroundExceptionHandler(Thread.UncaughtExceptionHandler exceptionHandler) {
            return set("backgroundExceptionHandler", exceptionHandler);
        }

        /**
         * Use the provided file store instead of the default one.
         * <p>
         * File stores passed in this way need to be open. They are not closed
         * when closing the store.
         * <p>
         * Please note that any kind of store (including an off-heap store) is
         * considered a "persistence", while an "in-memory store" means objects
         * are not persisted and fully kept in the JVM heap.
         * 
         * @param store the file store
         * @return this
         */
        public Builder fileStore(FileStore store) {
            return set("fileStore", store);
        }

        /**
         * Open the store.
         * 
         * @return the opened store
         */
        public AOStore open() {
            return new AOStore(config);
        }

        @Override
        public String toString() {
            return DataUtils.appendMap(new StringBuilder(), config).toString();
        }

        /**
         * Read the configuration from a string.
         * 
         * @param s the string representation
         * @return the builder
         */
        public static Builder fromString(String s) {
            HashMap<String, String> config = DataUtils.parseMap(s);
            Builder builder = new Builder();
            builder.config.putAll(config);
            return builder;
        }

    }
}
