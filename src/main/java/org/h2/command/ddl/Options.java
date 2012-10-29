package org.h2.command.ddl;

import java.util.ArrayList;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.h2.command.CommandInterface;
import org.h2.engine.Session;
import org.h2.expression.Expression;

public class Options extends DefineCommand {
	private ArrayList<String> optionNames;
	private ArrayList<Expression> optionValues;

	public Options(Session session, ArrayList<String> optionNames, ArrayList<Expression> optionValues) {
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

	public ArrayList<Expression> getOptionValues() {
		return optionValues;
	}

	public void setOptionValues(ArrayList<Expression> optionValues) {
		this.optionValues = optionValues;
	}

	@Override
	public int getType() {
		return CommandInterface.UNKNOWN;
	}

	public void initOptions(HColumnDescriptor hcd) {
		if (optionNames != null) {
			for (int i = 0, len = optionNames.size(); i < len; i++) {
				hcd.setValue(optionNames.get(i), optionValues.get(i).getValue(session).getString());
			}
		}
	}

	public void initOptions(HTableDescriptor htd) {
		if (optionNames != null) {
			for (int i = 0, len = optionNames.size(); i < len; i++) {
				htd.setValue(optionNames.get(i), optionValues.get(i).getValue(session).getString());
			}
		}
	}
}
