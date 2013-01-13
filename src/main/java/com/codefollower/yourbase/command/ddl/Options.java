package com.codefollower.yourbase.command.ddl;

import java.util.ArrayList;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import com.codefollower.h2.command.CommandInterface;
import com.codefollower.h2.command.ddl.DefineCommand;
import com.codefollower.h2.engine.Session;

public class Options extends DefineCommand {
	public static final String ON_DEFAULT_COLUMN_FAMILY_NAME = "DEFAULT_COLUMN_FAMILY_NAME";
	public static final String ON_ROW_KEY_NAME = "ROW_KEY_NAME";

	public static final String DEFAULT_ROW_KEY_NAME = "_ROWKEY_";

	private ArrayList<String> optionNames;
	private ArrayList<String> optionValues;

	public Options(Session session, ArrayList<String> optionNames, ArrayList<String> optionValues) {
		super(session);
		this.optionNames = optionNames;
		this.optionValues = optionValues;
	}

	public ArrayList<String> getOptionNames() {
		return optionNames;
	}

	public void setOptionNames(ArrayList<String> optionNames) {
		this.optionNames = optionNames;
	}

	public ArrayList<String> getOptionValues() {
		return optionValues;
	}

	public void setOptionValues(ArrayList<String> optionValues) {
		this.optionValues = optionValues;
	}

	@Override
	public int getType() {
		return CommandInterface.UNKNOWN;
	}

	public void initOptions(HColumnDescriptor hcd) {
		if (optionNames != null) {
			for (int i = 0, len = optionNames.size(); i < len; i++) {
				hcd.setValue(optionNames.get(i), optionValues.get(i));
			}
		}
	}

	public void initOptions(HTableDescriptor htd) {
		if (optionNames != null) {
			for (int i = 0, len = optionNames.size(); i < len; i++) {
				htd.setValue(optionNames.get(i), optionValues.get(i));
			}
		}
	}

	public String getDefaultColumnFamilyName() {
		if (optionNames != null)
			for (int i = 0, len = optionNames.size(); i < len; i++) {
				if (ON_DEFAULT_COLUMN_FAMILY_NAME.equalsIgnoreCase(optionNames.get(i)))
					return optionValues.get(i);
			}

		return null;
	}

	public String getRowKeyName() {
		if (optionNames != null)
			for (int i = 0, len = optionNames.size(); i < len; i++) {
				if (ON_ROW_KEY_NAME.equalsIgnoreCase(optionNames.get(i)))
					return optionValues.get(i);
			}

		return DEFAULT_ROW_KEY_NAME;
	}
}
