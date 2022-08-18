/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.TraceModuleType;
import org.lealone.common.util.MathUtils;
import org.lealone.common.util.Utils;
import org.lealone.db.Database;
import org.lealone.db.SysProperties;
import org.lealone.db.api.DatabaseEventListener;
import org.lealone.db.result.Row;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageEngine;

/**
 * @author H2 Group
 * @author zhh
 */
public class IndexRebuilder implements Runnable {

    private final ServerSession session;
    private final StorageEngine storageEngine;
    private final Table table;
    private final Index index;

    public IndexRebuilder(ServerSession session, StorageEngine storageEngine, Table table, Index index) {
        this.session = session;
        this.storageEngine = storageEngine;
        this.table = table;
        this.index = index;
    }

    @Override
    public void run() {
        rebuild();
    }

    public void rebuild() {
        try {
            if (index.isInMemory()) {
                // in-memory
                rebuildIndexBuffered();
            } else {
                rebuildIndexBlockMerge();
            }
        } catch (DbException e) {
            table.getSchema().freeUniqueName(index.getName());
            try {
                index.remove(session);
            } catch (DbException e2) {
                // this could happen, for example on failure in the storage
                // but if that is not the case it means
                // there is something wrong with the database
                session.getTrace().setType(TraceModuleType.TABLE).error(e2, "could not remove index");
                throw e2;
            }
            throw e;
        }
    }

    private void rebuildIndexBuffered() {
        Index scan = table.getScanIndex(session);
        long remaining = scan.getRowCount(session);
        long total = remaining;
        Cursor cursor = scan.find(session, null, null);
        long i = 0;
        Database database = table.getSchema().getDatabase();
        int bufferSize = (int) Math.min(total, database.getMaxMemoryRows());
        ArrayList<Row> buffer = new ArrayList<>(bufferSize);
        String n = table.getName() + ":" + index.getName();
        int t = MathUtils.convertLongToInt(total);
        while (cursor.next()) {
            Row row = cursor.get();
            buffer.add(row);
            database.setProgress(DatabaseEventListener.STATE_CREATE_INDEX, n,
                    MathUtils.convertLongToInt(i++), t);
            if (buffer.size() >= bufferSize) {
                addRowsToIndex(session, buffer, index);
            }
            remaining--;
        }
        addRowsToIndex(session, buffer, index);
        if (SysProperties.CHECK && remaining != 0) {
            DbException.throwInternalError("rowcount remaining=" + remaining + " " + table.getName());
        }
    }

    private void rebuildIndexBlockMerge() {
        // Read entries in memory, sort them, write to a new map (in sorted
        // order); repeat (using a new map for every block of 1 MB) until all
        // record are read. Merge all maps to the target (using merge sort;
        // duplicates are detected in the target). For randomly ordered data,
        // this should use relatively few write operations.
        // A possible optimization is: change the buffer size from "row count"
        // to "amount of memory", and buffer index keys instead of rows.
        Index scan = table.getScanIndex(session);
        long remaining = scan.getRowCount(session);
        long total = remaining;
        Cursor cursor = scan.find(session, null, null);
        long i = 0;
        Database database = table.getSchema().getDatabase();
        Storage storage = database.getStorage(storageEngine);
        int bufferSize = database.getMaxMemoryRows() / 2;
        ArrayList<Row> buffer = new ArrayList<>(bufferSize);
        String n = table.getName() + ":" + index.getName();
        int t = MathUtils.convertLongToInt(total);
        ArrayList<String> bufferNames = Utils.newSmallArrayList();
        while (cursor.next()) {
            Row row = cursor.get();
            buffer.add(row);
            database.setProgress(DatabaseEventListener.STATE_CREATE_INDEX, n,
                    MathUtils.convertLongToInt(i++), t);
            if (buffer.size() >= bufferSize) {
                sortRows(buffer, index);
                String mapName = storage.nextTemporaryMapName();
                index.addRowsToBuffer(session, buffer, mapName);
                bufferNames.add(mapName);
                buffer.clear();
            }
            remaining--;
        }
        sortRows(buffer, index);
        if (bufferNames.size() > 0) {
            String mapName = storage.nextTemporaryMapName();
            index.addRowsToBuffer(session, buffer, mapName);
            bufferNames.add(mapName);
            buffer.clear();
            index.addBufferedRows(session, bufferNames);
        } else {
            addRowsToIndex(session, buffer, index);
        }
        if (SysProperties.CHECK && remaining != 0) {
            DbException.throwInternalError("rowcount remaining=" + remaining + " " + table.getName());
        }
    }

    private static void addRowsToIndex(ServerSession session, ArrayList<Row> list, Index index) {
        sortRows(list, index);
        for (Row row : list) {
            index.add(session, row);
        }
        list.clear();
    }

    private static void sortRows(ArrayList<Row> list, final Index index) {
        Collections.sort(list, new Comparator<Row>() {
            @Override
            public int compare(Row r1, Row r2) {
                return index.compareRows(r1, r2);
            }
        });
    }
}
