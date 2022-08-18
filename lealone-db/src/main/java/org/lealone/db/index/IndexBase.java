/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.index;

import java.util.List;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StatementBuilder;
import org.lealone.common.util.StringUtils;
import org.lealone.db.Constants;
import org.lealone.db.DbObjectType;
import org.lealone.db.Mode;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.result.SortOrder;
import org.lealone.db.schema.SchemaObjectBase;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.storage.CursorParameters;

/**
 * Most index implementations extend the base index.
 * 
 * @author H2 Group
 * @author zhh
 */
public abstract class IndexBase extends SchemaObjectBase implements Index {

    protected final Table table;
    protected final IndexType indexType;
    protected IndexColumn[] indexColumns;
    protected Column[] columns;
    protected int[] columnIds;

    /**
     * Initialize the base index.
     *
     * @param table the table
     * @param id the object id
     * @param name the index name
     * @param indexType the index type
     * @param indexColumns the columns that are indexed or null if this is not yet known
     */
    protected IndexBase(Table table, int id, String name, IndexType indexType,
            IndexColumn[] indexColumns) {
        super(table.getSchema(), id, name);
        this.table = table;
        this.indexType = indexType;
        this.indexColumns = indexColumns;
        if (indexColumns != null) {
            int len = indexColumns.length;
            columns = new Column[len];
            columnIds = new int[len];
            for (int i = 0; i < len; i++) {
                Column col = indexColumns[i].column;
                columns[i] = col;
                columnIds[i] = col.getColumnId();
            }
        }
    }

    @Override
    public DbObjectType getType() {
        return DbObjectType.INDEX;
    }

    @Override
    public Table getTable() {
        return table;
    }

    @Override
    public IndexType getIndexType() {
        return indexType;
    }

    @Override
    public IndexColumn[] getIndexColumns() {
        return indexColumns;
    }

    @Override
    public Column[] getColumns() {
        return columns;
    }

    @Override
    public int[] getColumnIds() {
        return columnIds;
    }

    @Override
    public int getColumnIndex(Column col) { // 并不是返回列id，而是索引字段列表中的位置
        for (int i = 0, len = columns.length; i < len; i++) {
            if (columns[i].equals(col)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public String getPlanSQL() {
        return getSQL();
    }

    /**
     * Create a duplicate key exception with a message that contains the index name.
     *
     * @return the exception
     */
    protected DbException getDuplicateKeyException() {
        return getDuplicateKeyException(null);
    }

    /**
     * Create a duplicate key exception with a message that contains the index name.
     *
     * @param key the key values
     * @return the exception
     */
    protected DbException getDuplicateKeyException(String key) {
        String sql = getName() + " ON " + table.getSQL() + "(" + getColumnListSQL() + ")";
        if (key != null) {
            sql += " VALUES " + key;
        }
        return DbException.get(ErrorCode.DUPLICATE_KEY_1, sql);
    }

    @Override
    public Cursor find(ServerSession session, CursorParameters<SearchRow> parameters) {
        return find(session, parameters.from, parameters.to);
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public Cursor findFirstOrLast(ServerSession session, boolean first) {
        throw DbException.getUnsupportedException("findFirstOrLast");
    }

    @Override
    public boolean supportsDistinctQuery() {
        return false;
    }

    @Override
    public Cursor findDistinct(ServerSession session) {
        throw DbException.getUnsupportedException("findDistinct");
    }

    @Override
    public boolean canScan() {
        return true;
    }

    @Override
    public boolean isRowIdIndex() {
        return false;
    }

    @Override
    public Row getRow(ServerSession session, long key) {
        throw DbException.getUnsupportedException(toString());
    }

    @Override
    public int compareRows(SearchRow rowData, SearchRow compare) { // 只比较索引字段，并不一定是所有字段
        if (rowData == compare) {
            return 0;
        }
        for (int i = 0, len = indexColumns.length; i < len; i++) {
            int index = columnIds[i];

            Value v1 = rowData.getValue(index);
            Value v2 = compare.getValue(index);
            // 只要compare中有null值就认为无法比较，直接认为rowData和compare相等(通常在查询时在where中再比较)
            if (v1 == null || v2 == null) {
                // can't compare further
                return 0;
            }
            int c = compareValues(v1, v2, indexColumns[i].sortType);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    private int compareValues(Value a, Value b, int sortType) {
        if (a == b) {
            return 0;
        }
        boolean aNull = a == null, bNull = b == null;
        if (aNull || bNull) {
            return SortOrder.compareNull(aNull, sortType);
        }
        int comp = table.compareTypeSafe(a, b);
        if ((sortType & SortOrder.DESCENDING) != 0) { // 降序时，把比较结果反过来
            comp = -comp;
        }
        return comp;
    }

    /**
     * Compare the positions of two rows.
     *
     * @param rowData the first row
     * @param compare the second row
     * @return 0 if both rows are equal, -1 if the first row is smaller, otherwise 1
     */
    protected int compareKeys(SearchRow rowData, SearchRow compare) {
        long k1 = rowData.getKey();
        long k2 = compare.getKey();
        if (k1 == k2) {
            return 0;
        }
        return k1 > k2 ? 1 : -1;
    }

    /**
     * Calculate the cost for the given mask as if this index was a typical
     * b-tree range index. This is the estimated cost required to search one
     * row, and then iterate over the given number of rows.
     *
     * @param masks the search mask
     * @param rowCount the number of rows in the index
     * @param filter the table filter
     * @param sortOrder the sort order
     * @return the estimated cost
     */
    // 代价的计算总体上是围绕行数进行的
    protected long getCostRangeIndex(int[] masks, long rowCount, SortOrder sortOrder) {
        rowCount += Constants.COST_ROW_OFFSET;
        long cost = rowCount;
        long rows = rowCount;
        int totalSelectivity = 0;
        if (masks == null) {
            return cost;
        }
        for (int i = 0, len = columns.length; i < len; i++) {
            Column column = columns[i];
            int index = column.getColumnId();
            int mask = masks[index];
            // 代价比较:
            // EQUALITY < RANGE < END < START
            // 如果索引字段列表的第一个字段在Where中是RANGE、START、END，那么索引字段列表中的其他字段就不需要再计算cost了，
            // 如果是EQUALITY，则还可以继续计算cost，rows变量的值会变小，cost也会变小
            // 这里为什么不直接用(mask == IndexCondition.EQUALITY)？
            // 因为id=40 AND id>30会生成两个索引条件，
            // 在TableFilter.getBestPlanItem中合成一个mask为3(IndexCondition.EQUALITY|IndexCondition.START)
            if ((mask & IndexConditionType.EQUALITY) == IndexConditionType.EQUALITY) {
                // 索引字段列表中的最后一个在where当中是EQUALITY，且此索引是唯一索引时，cost直接是3
                // 因为如果最后一个索引字段是EQUALITY，说明前面的字段全是EQUALITY，
                // 如果是唯一索引则rowCount / distinctRows是1，所以rows = Math.max(rowCount / distinctRows, 1)=1
                // 所以cost = 2 + rows = 3
                if (i == columns.length - 1 && getIndexType().isUnique()) {
                    cost = 3;
                    break;
                }
                totalSelectivity = 100
                        - ((100 - totalSelectivity) * (100 - column.getSelectivity()) / 100);
                long distinctRows = rowCount * totalSelectivity / 100; // totalSelectivity变大时distinctRows变大
                if (distinctRows <= 0) {
                    distinctRows = 1;
                }
                rows = Math.max(rowCount / distinctRows, 1); // distinctRows变大，则rowCount / distinctRows变小，rows也变小
                cost = 2 + rows; // rows也变小，所以cost也变小
            } else if ((mask & IndexConditionType.RANGE) == IndexConditionType.RANGE) { // 见TableFilter.getBestPlanItem中的注释
                cost = 2 + rows / 4; // rows开始时加了1000，所以rows / 4总是大于1的
                break;
            } else if ((mask & IndexConditionType.START) == IndexConditionType.START) {
                cost = 2 + rows / 3;
                break;
            } else if ((mask & IndexConditionType.END) == IndexConditionType.END) { // "<="的代价要小于">="
                cost = rows / 3;
                break;
            } else {
                break;
            }
        }
        // if the ORDER BY clause matches the ordering of this index,
        // it will be cheaper than another index, so adjust the cost accordingly

        // order by中的字段和排序方式与索引字段相同时，cost再减去排序字段个数
        // 注意：排序字段个数不管比索引字段个数多还是少都是没问题的，这里只是尽量匹配
        if (sortOrder != null) {
            boolean sortOrderMatches = true;
            int coveringCount = 0;
            int[] sortTypes = sortOrder.getSortTypes();
            for (int i = 0, len = sortTypes.length; i < len; i++) {
                if (i >= indexColumns.length) {
                    // we can still use this index if we are sorting by more
                    // than it's columns, it's just that the coveringCount
                    // is lower than with an index that contains
                    // more of the order by columns
                    break;
                }
                Column col = sortOrder.getColumn(i, table);
                if (col == null) {
                    sortOrderMatches = false;
                    break;
                }
                IndexColumn indexCol = indexColumns[i];
                if (col != indexCol.column) {
                    sortOrderMatches = false;
                    break;
                }
                int sortType = sortTypes[i];
                if (sortType != indexCol.sortType) {
                    sortOrderMatches = false;
                    break;
                }
                coveringCount++;
            }
            if (sortOrderMatches) {
                // "coveringCount" makes sure that when we have two
                // or more covering indexes, we choose the one
                // that covers more
                cost -= coveringCount;
            }
        }
        return cost;
    }

    /**
     * Check if one of the columns is NULL and multiple rows with NULL are
     * allowed using the current compatibility mode for unique indexes. Note:
     * NULL behavior is complicated in SQL.
     *
     * @param newRow the row to check
     * @return true if one of the columns is null and multiple nulls in unique
     *         indexes are allowed
     */
    protected boolean containsNullAndAllowMultipleNull(SearchRow newRow) {
        Mode mode = database.getMode();
        if (mode.uniqueIndexSingleNull) {
            // 1. 对于唯一索引，必须完全唯一，适用于Derby/HSQLDB/MSSQLServer
            // 不允许出现:
            // (x, null)
            // (x, null)
            // 也不允许出现:
            // (null, null)
            // (null, null)
            return false;
        } else if (mode.uniqueIndexSingleNullExceptAllColumnsAreNull) {
            // 2. 对于唯一索引，索引记录可以全为null，适用于Oracle

            // 不允许出现:
            // (x, null)
            // (x, null)
            // 但是允许出现:
            // (null, null)
            // (null, null)
            for (int index : columnIds) {
                Value v = newRow.getValue(index);
                if (v != ValueNull.INSTANCE) {
                    return false;
                }
            }
            return true;
        }
        // 3. 对于唯一索引，只要一个为null，就是合法的，适用于REGULAR(即H2)/DB2/MySQL/PostgreSQL

        // 允许出现:
        // (x, null)
        // (x, null)
        // 也允许出现:
        // (null, null)
        // (null, null)

        // 也就是说，只要相同的两条索引记录包含null即可
        for (int index : columnIds) {
            Value v = newRow.getValue(index);
            if (v == ValueNull.INSTANCE) {
                return true;
            }
        }
        // 4. 对于唯一索引，没有null时是不允许出现两条相同的索引记录的
        return false;
    }

    /**
     * Check that the index columns are not CLOB or BLOB.
     *
     * @param columns the columns
     */
    protected static void checkIndexColumnTypes(IndexColumn[] columns) {
        for (IndexColumn c : columns) {
            int type = c.column.getType();
            if (type == Value.CLOB || type == Value.BLOB) {
                throw DbException.getUnsupportedException(
                        "Index on BLOB or CLOB column: " + c.column.getCreateSQL());
            }
        }
    }

    @Override
    public void close(ServerSession session) {
        // nothing to do
    }

    @Override
    public void remove(ServerSession session) {
        throw DbException.getUnsupportedException("remove index");
    }

    @Override
    public void truncate(ServerSession session) {
        throw DbException.getUnsupportedException("truncate index");
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public long getMemorySpaceUsed() {
        return 0;
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public boolean isInMemory() {
        return false;
    }

    @Override
    public void addRowsToBuffer(ServerSession session, List<Row> rows, String bufferName) {
        throw DbException.getUnsupportedException("addRowsToBuffer");
    }

    @Override
    public void addBufferedRows(ServerSession session, List<String> bufferNames) {
        throw DbException.getUnsupportedException("addBufferedRows");
    }

    // 以下是DbObject和SchemaObject接口的api实现

    @Override
    public String getCreateSQL() {
        StringBuilder buff = new StringBuilder("CREATE ");
        buff.append(indexType.getSQL());
        buff.append(' ');
        if (table.isHidden()) {
            buff.append("IF NOT EXISTS ");
        }
        buff.append(getSQL());
        buff.append(" ON ").append(table.getSQL());
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        buff.append('(').append(getColumnListSQL()).append(')');
        return buff.toString();
    }

    /**
     * Get the list of columns as a string.
     *
     * @return the list of columns
     */
    private String getColumnListSQL() {
        StatementBuilder buff = new StatementBuilder();
        for (IndexColumn c : indexColumns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        return buff.toString();
    }

    @Override
    public void removeChildrenAndResources(ServerSession session, DbObjectLock lock) {
        table.removeIndex(this);
        remove(session);
    }

    @Override
    public void checkRename() {
        // throw DbException.getUnsupportedException("checkRename");
    }

    @Override
    public boolean isHidden() {
        return table.isHidden();
    }
}
