/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction.aote.log;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import com.lealone.db.lock.Lockable;
import com.lealone.storage.StorageMap;
import com.lealone.storage.StorageMap.RedoLogBuffer;
import com.lealone.transaction.aote.AOTransaction;
import com.lealone.transaction.aote.AOTransactionEngine;
import com.lealone.transaction.aote.log.UndoLogRecord.KeyOnlyULR;
import com.lealone.transaction.aote.log.UndoLogRecord.KeyValueULR;

// 单个ScheduleService线程负责增加UndoLogRecord，如果事务涉及多个表，可能有多个FsyncService线程写RedoLog
public class UndoLog {

    private final AOTransaction t;

    private int logId;
    private UndoLogRecord first;// 指向最早加进来的，执行commit时从first开始遍历
    private UndoLogRecord last; // 总是指向新增加的，执行rollback时从first开始遍历

    // 保存需要写RedoLog的StorageMap，索引或内存表对应的StorageMap不需要写RedoLog
    private Map<StorageMap<?, ?>, AtomicBoolean> maps;
    private StorageMap<?, ?> lastMap; // 在大多数场景下，一个事务只涉及一张表，所以不需要增加到maps字段
    private Set<Integer> redoLogServiceIndexs;
    private int lastLogServiceIndex = -1;

    public UndoLog(AOTransaction t) {
        this.t = t;
    }

    public int getLogId() {
        return logId;
    }

    public UndoLogRecord getFirst() {
        return first;
    }

    public int size() {
        return logId;
    }

    public boolean isEmpty() {
        return logId == 0;
    }

    public boolean isNotEmpty() {
        return logId != 0;
    }

    public long getTransactionId() {
        return t.getTransactionId();
    }

    public boolean isMultiMaps() {
        return lastMap == null;
    }

    public Map<StorageMap<?, ?>, AtomicBoolean> getMaps() {
        return maps;
    }

    public Set<Integer> getRedoLogServiceIndexs() {
        return redoLogServiceIndexs;
    }

    // 让ScheduleService线程先执行，放在getMaps/getRedoLogServiceIndexs让多个FsyncService线程执行有并发问题
    public void prepareWrite() {
        if (maps == null) {
            maps = new HashMap<>();
            maps.put(lastMap, new AtomicBoolean(false));
        }
        if (redoLogServiceIndexs == null) {
            redoLogServiceIndexs = new HashSet<>();
            redoLogServiceIndexs.add(lastLogServiceIndex);
        }
    }

    public UndoLogRecord add(StorageMap<?, ?> map, Object key, Lockable lockable, Object oldValue) {
        if (map.getKeyType().isKeyOnly()) {
            return add(new KeyOnlyULR(map, key, lockable, oldValue));
        } else {
            int logServiceIndex;
            if (map.isInMemory()) {
                logServiceIndex = -1;
            } else {
                logServiceIndex = map.getRedoLogServiceIndex();
                if (logServiceIndex >= 0) {
                    if (redoLogServiceIndexs != null) {
                        redoLogServiceIndexs.add(logServiceIndex);
                    } else {
                        if (lastLogServiceIndex < 0) {
                            lastLogServiceIndex = logServiceIndex;
                        } else if (lastLogServiceIndex != logServiceIndex) {
                            redoLogServiceIndexs = new ConcurrentSkipListSet<>();
                            redoLogServiceIndexs.add(lastLogServiceIndex);
                            redoLogServiceIndexs.add(logServiceIndex);
                        }
                    }
                    if (maps != null) {
                        maps.put(map, new AtomicBoolean(false));
                    } else {
                        if (lastMap == null) {
                            lastMap = map;
                        } else if (lastMap != map) {
                            maps = new ConcurrentHashMap<>();
                            maps.put(map, new AtomicBoolean(false));
                            maps.put(lastMap, new AtomicBoolean(false));
                            lastMap = null; // 在isMultiMaps()中判断lastMap为null就能返回true
                        }
                    }
                }
            }
            return add(new KeyValueULR(map, key, lockable, oldValue, logServiceIndex));
        }
    }

    private UndoLogRecord add(UndoLogRecord r) {
        if (first == null) {
            first = last = r;
        } else {
            last.next = r;
            r.prev = last;
            last = r;
        }
        logId++;
        return r;
    }

    private UndoLogRecord removeLast() {
        UndoLogRecord r = last;
        if (last != null) {
            if (last.prev != null)
                last.prev.next = null;
            last = last.prev;
            if (last == null) {
                first = null;
            }
            --logId;
        }
        return r;
    }

    public int commit(AOTransactionEngine te) {
        UndoLogRecord r = first;
        while (r != null) {
            r.commit(te);
            r = r.next;
        }
        return logId;
    }

    public void rollbackTo(AOTransactionEngine te, int toLogId) {
        while (logId > toLogId) {
            UndoLogRecord r = removeLast();
            r.rollback(te);
        }
    }

    public int writeForRedo(Map<String, RedoLogBuffer> logs, int logServiceIndex) {
        int len = 0;
        UndoLogRecord r = first;
        while (r != null) {
            len += r.writeForRedo(logs, logServiceIndex, this);
            r = r.next;
        }
        return len;
    }
}
