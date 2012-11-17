package org.h2.index;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;
import org.h2.value.ValueString;

public class HBaseTableCursor implements Cursor {
	private final Session session;
	private long scannerId;
	private Result[] result;
	private int length = 0;

	public HBaseTableCursor(Session session) {
		this.session = session;
		try {
			scannerId = session.getRegionServer().openScanner(session.getRegionName(), new Scan());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Row get() {
		if (result == null || length >= result.length) {
			try {
				result = session.getRegionServer().next(scannerId, session.getFetchSize());
				length = 0;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (result != null && length < result.length) {
			Result r = result[length++];
			KeyValue[] kvs = r.raw();
			int size = kvs.length;
			Value[] data = new Value[size];

			for (int i = 0; i < size; i++) {
				data[i] = ValueString.get(Bytes.toString(kvs[i].getValue()));
			}
			return new Row(data, Row.MEMORY_CALCULATE);
		}
		return null;
	}

	@Override
	public SearchRow getSearchRow() {
		return get();
	}

	@Override
	public boolean next() {
		if (result != null && length < result.length)
			return true;

		try {
			result = session.getRegionServer().next(scannerId, session.getFetchSize());
			length = 0;
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (result != null && result.length > 0)
			return true;

		return false;
	}

	@Override
	public boolean previous() {
		return false;
	}

}
