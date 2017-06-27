/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.result;

import java.util.ArrayList;

import org.lealone.common.util.New;
import org.lealone.db.Constants;
import org.lealone.db.DataBuffer;
import org.lealone.db.Database;
import org.lealone.db.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.storage.fs.FileStorage;

/**
 * A list of rows. If the list grows too large, it is buffered to disk
 * automatically.
 */
public class RowList {

    private final ServerSession session;
    private final ArrayList<Row> list = New.arrayList();
    private int size;
    private int index, listIndex;
    private FileStorage file;
    private DataBuffer rowBuff;
    private ArrayList<Value> lobs;
    private final int maxMemory;
    private int memory;
    private boolean written;
    private boolean readUncached;

    /**
     * Construct a new row list for this session.
     *
     * @param session the session
     */
    public RowList(ServerSession session) {
        this.session = session;
        if (session.getDatabase().isPersistent()) {
            maxMemory = session.getDatabase().getMaxOperationMemory();
        } else {
            maxMemory = 0;
        }
    }

    private void writeRow(DataBuffer buff, Row r) {
        buff.checkCapacity(1 + DataBuffer.LENGTH_INT * 8);
        buff.put((byte) 1);
        buff.putInt(r.getMemory());
        int columnCount = r.getColumnCount();
        buff.putInt(columnCount);
        buff.putLong(r.getKey());
        buff.putInt(r.getVersion());
        buff.putInt(r.isDeleted() ? 1 : 0);
        for (int i = 0; i < columnCount; i++) {
            Value v = r.getValue(i);
            buff.checkCapacity(1);
            if (v == null) {
                buff.put((byte) 0);
            } else {
                buff.put((byte) 1);
                if (v.getType() == Value.CLOB || v.getType() == Value.BLOB) {
                    // need to keep a reference to temporary lobs,
                    // otherwise the temp file is deleted
                    if (v.getSmall() == null && v.getTableId() == 0) {
                        if (lobs == null) {
                            lobs = New.arrayList();
                        }
                        // need to create a copy, otherwise,
                        // if stored multiple times, it may be renamed
                        // and then not found
                        v = v.copyToTemp();
                        lobs.add(v);
                    }
                }
                buff.writeValue(v);
            }
        }
    }

    private void writeAllRows() {
        if (file == null) {
            Database db = session.getDatabase();
            String fileName = db.createTempFile();
            file = db.openFile(fileName, "rw", false);
            file.setCheckedWriting(false);
            file.seek(FileStorage.HEADER_LENGTH);
            rowBuff = DataBuffer.create(db, Constants.DEFAULT_PAGE_SIZE);
            file.seek(FileStorage.HEADER_LENGTH);
        }
        DataBuffer buff = rowBuff;
        initBuffer(buff);
        for (int i = 0, size = list.size(); i < size; i++) {
            if (i > 0 && buff.length() > Constants.IO_BUFFER_SIZE) {
                flushBuffer(buff);
                initBuffer(buff);
            }
            Row r = list.get(i);
            writeRow(buff, r);
        }
        flushBuffer(buff);
        file.autoDelete();
        list.clear();
        memory = 0;
    }

    private static void initBuffer(DataBuffer buff) {
        buff.reset();
        buff.putInt(0);
    }

    private void flushBuffer(DataBuffer buff) {
        buff.checkCapacity(1);
        buff.put((byte) 0);
        buff.fillAligned();
        buff.putInt(0, buff.length() / Constants.FILE_BLOCK_SIZE);
        file.write(buff.getBytes(), 0, buff.length());
    }

    /**
     * Add a row to the list.
     *
     * @param r the row to add
     */
    public void add(Row r) {
        list.add(r);
        memory += r.getMemory() + Constants.MEMORY_POINTER;
        if (maxMemory > 0 && memory > maxMemory) {
            writeAllRows();
        }
        size++;
    }

    /**
     * Remove all rows from the list.
     */
    public void reset() {
        index = 0;
        if (file != null) {
            listIndex = 0;
            if (!written) {
                writeAllRows();
                written = true;
            }
            list.clear();
            file.seek(FileStorage.HEADER_LENGTH);
        }
    }

    /**
     * Check if there are more rows in this list.
     *
     * @return true it there are more rows
     */
    public boolean hasNext() {
        return index < size;
    }

    private Row readRow(DataBuffer buff) {
        if (buff.readByte() == 0) {
            return null;
        }
        int mem = buff.getInt();
        int columnCount = buff.getInt();
        long key = buff.getLong();
        int version = buff.getInt();
        if (readUncached) {
            key = 0;
        }
        boolean deleted = buff.getInt() == 1;
        Value[] values = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            Value v;
            if (buff.readByte() == 0) {
                v = null;
            } else {
                v = buff.readValue();
                if (v.isLinked()) {
                    // the table id is 0 if it was linked when writing
                    // a temporary entry
                    if (v.getTableId() == 0) {
                        session.unlinkAtCommit(v);
                    }
                }
            }
            values[i] = v;
        }
        Row row = new Row(values, mem);
        row.setKey(key);
        row.setVersion(version);
        row.setDeleted(deleted);
        return row;
    }

    /**
     * Get the next row from the list.
     *
     * @return the next row
     */
    public Row next() {
        Row r;
        if (file == null) {
            r = list.get(index++);
        } else {
            if (listIndex >= list.size()) {
                list.clear();
                listIndex = 0;
                DataBuffer buff = rowBuff;
                buff.reset();
                int min = Constants.FILE_BLOCK_SIZE;
                file.readFully(buff.getBytes(), 0, min);
                int len = buff.getInt() * Constants.FILE_BLOCK_SIZE;
                buff.checkCapacity(len);
                if (len - min > 0) {
                    file.readFully(buff.getBytes(), min, len - min);
                }
                while (true) {
                    r = readRow(buff);
                    if (r == null) {
                        break;
                    }
                    list.add(r);
                }
            }
            index++;
            r = list.get(listIndex++);
        }
        return r;
    }

    /**
     * Get the number of rows in this list.
     *
     * @return the number of rows
     */
    public int size() {
        return size;
    }

    /**
     * Do not use the cache.
     */
    public void invalidateCache() {
        readUncached = true;
    }

    /**
     * Close the result list and delete the temporary file.
     */
    public void close() {
        if (file != null) {
            file.autoDelete();
            file.closeAndDeleteSilently();
            file = null;
            rowBuff = null;
        }
    }

}
