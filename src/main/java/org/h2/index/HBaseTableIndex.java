package org.h2.index;

import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.HBaseTable;
import org.h2.table.IndexColumn;

public class HBaseTableIndex extends BaseIndex {

	public HBaseTableIndex(HBaseTable table, int id, IndexColumn[] columns, IndexType indexType) {
		initBaseIndex(table, id, table.getName() + "_DATA", columns, indexType);
	}

	@Override
	public void close(Session session) {
	}

	@Override
	public void add(Session session, Row row) {
	}

	@Override
	public void remove(Session session, Row row) {
	}

	@Override
	public Cursor find(Session session, SearchRow first, SearchRow last) {
		return new HBaseTableCursor(session, first, last);
	}

	@Override
	public double getCost(Session session, int[] masks) {
		return 0;
	}

	@Override
	public void remove(Session session) {
	}

	@Override
	public void truncate(Session session) {
	}

	@Override
	public boolean canGetFirstOrLast() {
		return false;
	}

	@Override
	public Cursor findFirstOrLast(Session session, boolean first) {
		return new HBaseTableCursor(session, null, null);
	}

	@Override
	public boolean needRebuild() {
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
	public void checkRename() {
	}

}
