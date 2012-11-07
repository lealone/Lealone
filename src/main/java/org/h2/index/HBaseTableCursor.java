package org.h2.index;

import org.h2.result.Row;
import org.h2.result.SearchRow;

public class HBaseTableCursor implements Cursor {

	@Override
	public Row get() {
		return null;
	}

	@Override
	public SearchRow getSearchRow() {
		return null;
	}

	@Override
	public boolean next() {
		return false;
	}

	@Override
	public boolean previous() {
		return false;
	}

}
