/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.yourbase.mvstore.dbobject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.WeakHashMap;

import com.codefollower.yourbase.api.TableEngine;
import com.codefollower.yourbase.command.ddl.CreateTableData;
import com.codefollower.yourbase.constant.Constants;
import com.codefollower.yourbase.constant.ErrorCode;
import com.codefollower.yourbase.dbobject.table.TableBase;
import com.codefollower.yourbase.dbobject.table.TableEngineManager;
import com.codefollower.yourbase.engine.Database;
import com.codefollower.yourbase.message.DbException;
import com.codefollower.yourbase.mvstore.MVStore;
import com.codefollower.yourbase.util.New;

/**
 * A table engine that internally uses the MVStore.
 */
public class MVTableEngine implements TableEngine {
    public static final String NAME = "MVSTORE";
    private static final MVTableEngine INSTANCE = new MVTableEngine();
    static {
        TableEngineManager.registerTableEngine(INSTANCE);
    }
    static final Map<String, Store> STORES = new WeakHashMap<String, Store>();

    /**
     * Flush all changes.
     *
     * @param db the database
     */
    public static void flush(Database db) {
        String storeName = db.getDatabasePath();
        if (storeName == null) {
            return;
        }
        synchronized (STORES) {
            Store store = STORES.get(storeName);
            if (store == null) {
                return;
            }
            // TODO this stores uncommitted transactions as well
            store(store.getStore());
        }
    }

    public static Collection<Store> getStores() {
        return STORES.values();
    }

    @Override
    public TableBase createTable(CreateTableData data) {
        Database db = data.session.getDatabase();
        byte[] key = db.getFilePasswordHash();
        String storeName = db.getDatabasePath();
        MVStore.Builder builder = new MVStore.Builder();
        Store store;
        if (storeName == null) {
            store = new Store(db, builder.open());
        } else {
            synchronized (STORES) {
                store = STORES.get(storeName);
                if (store == null) {
                    builder.fileName(storeName + Constants.SUFFIX_MV_FILE);
                    if (db.isReadOnly()) {
                        builder.readOnly();
                    }
                    if (key != null) {
                        char[] password = new char[key.length];
                        for (int i = 0; i < key.length; i++) {
                            password[i] = (char) key[i];
                        }
                        builder.encryptionKey(password);
                    }
                    store = new Store(db, builder.open());
                    STORES.put(storeName, store);
                } else if (store.db != db) {
                    throw DbException.get(ErrorCode.DATABASE_ALREADY_OPEN_1, storeName);
                }
            }
        }
        MVTable table = new MVTable(data, storeName, store.getStore());
        store.openTables.add(table);
        table.init(data.session);
        return table;
    }

    /**
     * Close the table, and close the store if there are no remaining open
     * tables.
     *
     * @param storeName the store name
     * @param table the table
     */
    static void closeTable(String storeName, MVTable table) {
        synchronized (STORES) {
            Store store = STORES.get(storeName);
            if (store != null) {
                store.openTables.remove(table);
                if (store.openTables.size() == 0) {
                    store(store.getStore());
                    store.getStore().close();
                    STORES.remove(storeName);
                }
            }
        }
    }

    /**
     * Store the data if needed.
     *
     * @param store the store
     */
    static void store(MVStore store) {
        store.compact(50);
        store.store();
    }

    /**
     * A store with open tables.
     */
    public static class Store {

        /**
         * The database.
         */
        final Database db;

        /**
         * The list of open tables.
         */
        final ArrayList<MVTable> openTables = New.arrayList();

        /**
         * The store.
         */
        private final MVStore store;

        public Store(Database db, MVStore store) {
            this.db = db;
            this.store = store;
        }

        public MVStore getStore() {
            return store;
        }

    }

    @Override
    public String getName() {
        return NAME;
    }

}
