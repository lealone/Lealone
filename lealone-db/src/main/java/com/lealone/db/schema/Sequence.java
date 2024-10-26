/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.schema;

import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.DbObjectType;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.row.Row;
import com.lealone.db.session.ServerSession;
import com.lealone.db.session.SessionStatus;
import com.lealone.transaction.Transaction;

/**
 * A sequence is created using the statement
 * CREATE SEQUENCE
 *
 * @author H2 Group
 * @author zhh
 */
public class Sequence extends SchemaObjectBase {

    /**
     * The default cache size for sequences.
     */
    private static final int DEFAULT_CACHE_SIZE = 32;

    private AtomicLong value;
    private long valueWithMargin;
    private long increment;
    private long cacheSize;
    private long minValue;
    private long maxValue;
    private boolean cycle;
    private boolean belongsToTable;
    private boolean transactional;

    /**
     * The last valueWithMargin we flushed. We do a little dance with this to avoid an ABBA deadlock.
     */
    private long lastFlushValueWithMargin;

    /**
     * Creates a new sequence for an auto-increment column.
     *
     * @param schema the schema
     * @param id the object id
     * @param name the sequence name
     * @param startValue the first value to return
     * @param increment the increment count
     */
    public Sequence(Schema schema, int id, String name, long startValue, long increment) {
        this(schema, id, name, startValue, increment, null, null, null, false, true);
    }

    /**
     * Creates a new sequence.
     *
     * @param schema the schema
     * @param id the object id
     * @param name the sequence name
     * @param startValue the first value to return
     * @param increment the increment count
     * @param cacheSize the number of entries to pre-fetch
     * @param minValue the minimum value
     * @param maxValue the maximum value
     * @param cycle whether to jump back to the min value if needed
     * @param belongsToTable whether this sequence belongs to a table (for
     *            auto-increment columns)
     */
    public Sequence(Schema schema, int id, String name, Long startValue, Long increment, Long cacheSize,
            Long minValue, Long maxValue, boolean cycle, boolean belongsToTable) {
        super(schema, id, name);
        this.increment = increment != null ? increment : 1;
        this.minValue = minValue != null ? minValue : getDefaultMinValue(startValue, this.increment);
        this.maxValue = maxValue != null ? maxValue : getDefaultMaxValue(startValue, this.increment);
        Long value = startValue != null ? startValue : getDefaultStartValue(this.increment);
        this.valueWithMargin = value;
        this.cacheSize = cacheSize != null ? Math.max(1, cacheSize) : DEFAULT_CACHE_SIZE;
        this.cycle = cycle;
        this.belongsToTable = belongsToTable;
        if (!isValid(value, this.minValue, this.maxValue, this.increment)) {
            throw DbException.get(ErrorCode.SEQUENCE_ATTRIBUTES_INVALID, name, String.valueOf(value),
                    String.valueOf(this.minValue), String.valueOf(this.maxValue),
                    String.valueOf(this.increment));
        }
        this.value = new AtomicLong(value.longValue());
    }

    @Override
    public DbObjectType getType() {
        return DbObjectType.SEQUENCE;
    }

    public Sequence copy() {
        Sequence sequence = new Sequence(schema, id, name, value.get(), increment, cacheSize, minValue,
                maxValue, cycle, belongsToTable);
        sequence.setTransactional(transactional);
        return sequence;
    }

    public long getCurrentValue(ServerSession session) {
        Sequence oldSequence = this.oldSequence;
        if (transaction == session.getTransaction() || oldSequence == null)
            return value.get() - increment;
        return oldSequence.value.get() - oldSequence.increment;
    }

    public boolean getBelongsToTable() {
        return belongsToTable;
    }

    public void setBelongsToTable(boolean b) {
        this.belongsToTable = b;
    }

    public boolean isTransactional() {
        return transactional;
    }

    public void setTransactional(boolean transactional) {
        this.transactional = transactional;
    }

    public long getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(long cacheSize) {
        this.cacheSize = Math.max(1, cacheSize);
    }

    public long getIncrement() {
        return increment;
    }

    public long getMinValue() {
        return minValue;
    }

    public long getMaxValue() {
        return maxValue;
    }

    public boolean getCycle() {
        return cycle;
    }

    public void setCycle(boolean cycle) {
        this.cycle = cycle;
    }

    /**
     * Allows the start value, increment, min value and max value to be updated
     * atomically, including atomic validation. Useful because setting these
     * attributes one after the other could otherwise result in an invalid
     * sequence state (e.g. min value > max value, start value < min value,
     * etc).
     *
     * @param startValue the new start value (<code>null</code> if no change)
     * @param minValue the new min value (<code>null</code> if no change)
     * @param maxValue the new max value (<code>null</code> if no change)
     * @param increment the new increment (<code>null</code> if no change)
     */
    public void modify(ServerSession session, Long startValue, Long minValue, Long maxValue,
            Long increment, boolean copy, boolean locked) {
        if (!locked)
            tryLock(session, copy);
        modify(startValue, minValue, maxValue, increment);
        if (!locked)
            unlockIfNotTransactional(session);
    }

    public void modify(Long startValue, Long minValue, Long maxValue, Long increment) {
        if (startValue == null) {
            startValue = this.value.get();
        }
        if (minValue == null) {
            minValue = this.minValue;
        }
        if (maxValue == null) {
            maxValue = this.maxValue;
        }
        if (increment == null) {
            increment = this.increment;
        }
        if (!isValid(startValue, minValue, maxValue, increment)) {
            throw DbException.get(ErrorCode.SEQUENCE_ATTRIBUTES_INVALID, getName(),
                    String.valueOf(startValue), String.valueOf(minValue), String.valueOf(maxValue),
                    String.valueOf(increment));
        }
        this.value.set(startValue);
        this.valueWithMargin = startValue;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.increment = increment;
    }

    /**
     * Validates the specified prospective start value, min value, max value and
     * increment relative to each other, since each of their respective
     * validities are contingent on the values of the other parameters.
     *
     * @param value the prospective start value
     * @param minValue the prospective min value
     * @param maxValue the prospective max value
     * @param increment the prospective increment
     */
    private static boolean isValid(long value, long minValue, long maxValue, long increment) {
        return minValue <= value && maxValue >= value && maxValue > minValue && increment != 0 &&
        // Math.abs(increment) < maxValue - minValue
        // use BigInteger to avoid overflows when maxValue and minValue
        // are really big
                BigInteger.valueOf(increment).abs().compareTo(
                        BigInteger.valueOf(maxValue).subtract(BigInteger.valueOf(minValue))) < 0;
    }

    private static long getDefaultMinValue(Long startValue, long increment) {
        long v = increment >= 0 ? 1 : Long.MIN_VALUE;
        if (startValue != null && increment >= 0 && startValue < v) {
            v = startValue;
        }
        return v;
    }

    private static long getDefaultMaxValue(Long startValue, long increment) {
        long v = increment >= 0 ? Long.MAX_VALUE : -1;
        if (startValue != null && increment < 0 && startValue > v) {
            v = startValue;
        }
        return v;
    }

    private long getDefaultStartValue(long increment) {
        return increment >= 0 ? minValue : maxValue;
    }

    @Override
    public String getCreateSQL() {
        StringBuilder buff = new StringBuilder("CREATE SEQUENCE ");
        buff.append(getSQL()).append(" START WITH ").append(value);
        if (increment != 1) {
            buff.append(" INCREMENT BY ").append(increment);
        }
        if (minValue != getDefaultMinValue(value.get(), increment)) {
            buff.append(" MINVALUE ").append(minValue);
        }
        if (maxValue != getDefaultMaxValue(value.get(), increment)) {
            buff.append(" MAXVALUE ").append(maxValue);
        }
        if (cycle) {
            buff.append(" CYCLE");
        }
        if (cacheSize != DEFAULT_CACHE_SIZE) {
            buff.append(" CACHE ").append(cacheSize);
        }
        if (belongsToTable) {
            buff.append(" BELONGS_TO_TABLE");
        }
        if (transactional) {
            buff.append(" TRANSACTIONAL");
        } else {
            buff.append(" NOT_TRANSACTIONAL"); // 用NOT TRANSACTIONAL无法解析NOT
        }
        return buff.toString();
    }

    @Override
    public String getDropSQL() {
        if (getBelongsToTable()) {
            return null;
        }
        return "DROP SEQUENCE IF EXISTS " + getSQL();
    }

    /**
     * Get the next value for this sequence.
     *
     * @param session the session
     * @return the next value
     */
    public long getNext(ServerSession session) {
        tryLock(session);
        boolean needsFlush = false;
        long retVal;
        long flushValueWithMargin = -1;
        if ((increment > 0 && value.get() >= valueWithMargin)
                || (increment < 0 && value.get() <= valueWithMargin)) {
            valueWithMargin += increment * cacheSize;
            flushValueWithMargin = valueWithMargin;
            needsFlush = true;
        }
        if ((increment > 0 && value.get() > maxValue) || (increment < 0 && value.get() < minValue)) {
            if (cycle) {
                value.set(increment > 0 ? minValue : maxValue);
                valueWithMargin = value.get() + (increment * cacheSize);
                flushValueWithMargin = valueWithMargin;
                needsFlush = true;
            } else {
                throw DbException.get(ErrorCode.SEQUENCE_EXHAUSTED, getName());
            }
        }
        retVal = value.getAndAdd(increment);
        if (needsFlush) {
            flushInternal(session, flushValueWithMargin);
        }
        unlockIfNotTransactional(session);
        return retVal;
    }

    /**
     * Flush the current value to disk and close this object.
     */
    public void close() {
        flushWithoutMargin();
    }

    /**
     * Flush the current value to disk.
     */
    private void flushWithoutMargin() {
        if (valueWithMargin != value.get()) {
            valueWithMargin = value.get();
            flush(database.getSystemSession(), valueWithMargin, false);
            database.getSystemSession().commit();
        }
    }

    /**
     * Flush the current value, including the margin, to disk.
     *
     * @param session the session
     */
    public void flush(ServerSession session, long flushValueWithMargin, boolean locked) {
        if (!locked)
            tryLock(session);
        flushInternal(session, flushValueWithMargin);
        unlockIfNotTransactional(session);
        if (!locked)
            unlockIfNotTransactional(session);
    }

    private void flushInternal(ServerSession session, long flushValueWithMargin) {
        if (flushValueWithMargin == lastFlushValueWithMargin)
            return;
        // just for this case, use the value with the margin for the script
        long realValue = value.get();
        try {
            value.set(valueWithMargin);
            if (!isTemporary()) {
                schema.update(session, this, oldRow, lock);
            }
        } finally {
            value.set(realValue);
        }
        lastFlushValueWithMargin = flushValueWithMargin;
    }

    private Transaction transaction;
    private DbObjectLock lock;
    private Row oldRow;
    private Sequence oldSequence;

    public void setOldSequence(Sequence oldSequence) {
        this.oldSequence = oldSequence;
    }

    public DbObjectLock getLock() {
        return lock;
    }

    public Row getOldRow() {
        return oldRow;
    }

    public void tryLock(ServerSession session) {
        tryLock(session, true);
    }

    public void tryLock(ServerSession session, boolean copy) {
        if (transaction == session.getTransaction())
            return;
        DbObjectLock lock = schema.tryExclusiveLock(DbObjectType.SEQUENCE, session);
        if (lock == null) {
            onLocked(session);
        }
        Row oldRow = schema.tryLockSchemaObject(session, this, ErrorCode.SEQUENCE_NOT_FOUND_1);
        if (oldRow == null) {
            onLocked(session);
        }
        this.transaction = session.getTransaction();
        this.lock = lock;
        this.oldRow = oldRow;
        if (copy)
            this.oldSequence = copy();
        lock.addHandler(ar -> {
            this.transaction = null;
            this.lock = null;
            this.oldRow = null;
            if (copy) {
                if (!(ar.isSucceeded() && ar.getResult())) {
                    rollback(this.oldSequence);
                }
                this.oldSequence = null;
            }
        });
    }

    public void unlockIfNotTransactional(ServerSession session) {
        if (!transactional && transaction == session.getTransaction()) {
            // 释放两个锁
            session.unlockLast();
            session.unlockLast();
            session.wakeUpWaitingSchedulers(false);
        }
    }

    private void onLocked(ServerSession session) {
        // 让出执行权给其他session，然后会继续重式
        if (!transactional)
            session.setStatus(SessionStatus.STATEMENT_YIELDED);
        throw DbObjectLock.LOCKED_EXCEPTION;
    }

    private void rollback(Sequence oldSequence) {
        this.increment = oldSequence.increment;
        this.minValue = oldSequence.minValue;
        this.maxValue = oldSequence.maxValue;
        this.value = oldSequence.value;
        this.valueWithMargin = oldSequence.valueWithMargin;
        this.cacheSize = oldSequence.cacheSize;
        this.cycle = oldSequence.cycle;
        this.belongsToTable = oldSequence.belongsToTable;
        this.transactional = oldSequence.transactional;
    }

    @Override
    public void onUpdateComplete() {
        if (oldSequence != null) {
            // 只需要把id变为-1即可，不需要调用invalidate，后续的操作可能还要用其他字段
            oldSequence.id = -1;
            oldSequence = null;
        }
    }

    public Sequence getNewSequence(ServerSession session) {
        return getSchema().findSequence(session, getName());
    }
}
