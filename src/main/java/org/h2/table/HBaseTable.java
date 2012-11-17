package org.h2.table;

import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.h2.command.ddl.Options;
import org.h2.engine.Session;
import org.h2.index.HBaseTableIndex;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.util.StatementBuilder;
import org.h2.value.Value;

public class HBaseTable extends Table {
	private HTableDescriptor hTableDescriptor;
	private String rowKeyName;
	private Index scanIndex;

	public HBaseTable(Schema schema, int id, String name, boolean persistIndexes, boolean persistData) {
		super(schema, id, name, persistIndexes, persistData);

		scanIndex = new HBaseTableIndex(this, id, IndexColumn.wrap(getColumns()), IndexType.createScan(false));
	}

	public void setHTableDescriptor(HTableDescriptor hTableDescriptor) {
		this.hTableDescriptor = hTableDescriptor;
	}

	public String getDefaultColumnFamilyName() {
		return hTableDescriptor.getValue(Options.ON_DEFAULT_COLUMN_FAMILY_NAME);
	}

	public void setRowKeyName(String rowKeyName) {
		this.rowKeyName = rowKeyName;
	}

	public String getRowKeyName() {
		if (rowKeyName == null)
			return Options.DEFAULT_ROW_KEY_NAME;
		return this.rowKeyName;
	}

	@Override
	public void lock(Session session, boolean exclusive, boolean force) {

	}

	@Override
	public void close(Session session) {

	}

	@Override
	public void unlock(Session s) {

	}

	@Override
	public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
			boolean create, String indexComment) {

		return scanIndex;
	}

	@Override
	public void removeRow(Session session, Row row) {

	}

	@Override
	public void truncate(Session session) {

	}

	@Override
	public void addRow(Session session, Row row) {
	}

	@Override
	public void checkSupportAlter() {

	}

	@Override
	public String getTableType() {
		return "HBASE TABLE";
	}

	@Override
	public Index getScanIndex(Session session) {
		return scanIndex;
	}

	@Override
	public Index getUniqueIndex() {
		return scanIndex;
	}

	@Override
	public ArrayList<Index> getIndexes() {
		return null;
	}

	@Override
	public boolean isLockedExclusively() {
		return false;
	}

	@Override
	public long getMaxDataModificationId() {
		return 0;
	}

	@Override
	public boolean isDeterministic() {
		return false;
	}

	@Override
	public boolean canGetRowCount() {
		return false;
	}

	@Override
	public boolean canDrop() {
		return true;
	}

	@Override
	public long getRowCount(Session session) {
		return 0;
	}

	@Override
	public long getRowCountApproximation() {
		return 0;
	}

	@Override
	public String getCreateSQL() {
		return HBaseTable.getCreateSQL(hTableDescriptor, getSQL());
	}

	public static String getCreateSQL(HTableDescriptor hTableDescriptor, String tableName) {
		StatementBuilder buff = new StatementBuilder("CREATE HBASE TABLE IF NOT EXISTS ");
		buff.append(tableName);
		buff.append("(\n");
		buff.append("    OPTIONS(");
		boolean first = true;
		for (Entry<ImmutableBytesWritable, ImmutableBytesWritable> e : hTableDescriptor.getValues().entrySet()) {
			//buff.appendExceptFirst(",");
			if (first) {
				first = false;
			} else {
				buff.append(",");
			}
			buff.append(toS(e.getKey())).append("='").append(toS(e.getValue())).append("'");
		}
		buff.append(")");
		HColumnDescriptor[] hcds = hTableDescriptor.getColumnFamilies();
		String cfName;
		if (hcds != null && hcds.length > 0) {
			for (HColumnDescriptor hcd : hcds) {
				cfName = hcd.getNameAsString();
				buff.append(",\n    COLUMN FAMILY ").append(cfName);
				buff.append(" OPTIONS(");
				first = true;
				for (Entry<ImmutableBytesWritable, ImmutableBytesWritable> e : hcd.getValues().entrySet()) {
					if (first) {
						first = false;
					} else {
						buff.append(",");
					}
					buff.append(toS(e.getKey())).append("='").append(toS(e.getValue())).append("'");
				}
				buff.append(")");
			}
		}
		buff.append("\n)");
		return buff.toString();
	}

	@Override
	public String getDropSQL() {
		return "DROP HBASE TABLE IF EXISTS " + getSQL();
	}

	@Override
	public void checkRename() {

	}

	@Override
	public Column getColumn(String columnName) {
		return new Column(columnName, Value.STRING);
	}

	@Override
	public Column[] getColumns() {
		return new Column[0];
	}

	private static String toS(ImmutableBytesWritable v) {
		return Bytes.toString(v.get());
	}
}
