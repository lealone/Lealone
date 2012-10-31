package org.h2.table;

import java.util.ArrayList;

import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.value.Value;

public class DummyTable extends Table {

	public DummyTable(Schema schema, int id, String name, boolean persistIndexes, boolean persistData) {
		super(schema, id, name, persistIndexes, persistData);
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

		return null;
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

		return null;
	}

	@Override
	public Index getScanIndex(Session session) {

		return null;
	}

	@Override
	public Index getUniqueIndex() {

		return null;
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

		return false;
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

		return null;
	}

	@Override
	public String getDropSQL() {

		return null;
	}

	@Override
	public void checkRename() {

	}
	
	@Override
	public Column getColumn(String columnName) {
		return new Column(columnName, Value.STRING);
    }
}
