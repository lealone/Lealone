package org.h2.command.ddl;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.h2.command.CommandInterface;
import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.HBaseTable;
import org.h2.util.New;

public class CreateHBaseTable extends SchemaCommand {
	private boolean ifNotExists;
	private String tableName;
	private ArrayList<CreateColumnFamily> cfList = New.arrayList();
	private Options options;

	public CreateHBaseTable(Session session, Schema schema, String tableName) {
		super(session, schema);
		this.tableName = tableName;
	}

	public void setIfNotExists(boolean ifNotExists) {
		this.ifNotExists = ifNotExists;
	}

	public void setOptions(Options options) {
		this.options = options;
	}

	public void addCreateColumnFamily(CreateColumnFamily cf) {
		cfList.add(cf);
	}

	@Override
	public int getType() {
		return CommandInterface.CREATE_TABLE;
	}

	public int update() {
		if (!transactional) {
			session.commit(true);
		}

		if (getSchema().findTableOrView(session, tableName) != null) {
			if (ifNotExists) {
				return 0;
			}
			throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, tableName);
		}

		String defaultColumnFamilyName = null;
		String rowKeyName = null;
		if (options != null) {
			defaultColumnFamilyName = options.getDefaultColumnFamilyName();
			rowKeyName = options.getRowKeyName();
		}
		if (rowKeyName == null)
			rowKeyName = Options.DEFAULT_ROW_KEY_NAME;

		HTableDescriptor htd = new HTableDescriptor(tableName);
		for (CreateColumnFamily cf : cfList) {
			if (defaultColumnFamilyName == null)
				defaultColumnFamilyName = cf.getColumnFamilyName();
			htd.addFamily(cf.createHColumnDescriptor());
		}

		if (options != null) {
			options.initOptions(htd);
		}

		htd.setValue(Options.ON_ROW_KEY_NAME, rowKeyName);
		htd.setValue(Options.ON_DEFAULT_COLUMN_FAMILY_NAME, defaultColumnFamilyName.toUpperCase()); //H2默认转大写

		//TODO splitKeys
		try {
			if (session.getMaster() != null)
				session.getMaster().createTable(htd, null);
		} catch (IOException e) {
			throw DbException.convertIOException(e, "Failed to HMaster.createTable");
		}

		int id = getObjectId();
		HBaseTable table = new HBaseTable(getSchema(), id, tableName, true, true);
		table.setRowKeyName(rowKeyName);
		htd.setValue("OBJECT_ID", id + "");
		htd.setValue("OBJECT_NAME", table.getSQL());
		htd.setValue("OBJECT_TYPE", table.getType() + "");
		table.setHTableDescriptor(htd);
		Database db = session.getDatabase();
		db.lockMeta(session);
		db.addSchemaObject(session, table);

		return 0;
	}

}
