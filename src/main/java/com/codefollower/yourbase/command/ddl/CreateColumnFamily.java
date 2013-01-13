package com.codefollower.yourbase.command.ddl;

import org.apache.hadoop.hbase.HColumnDescriptor;

import com.codefollower.h2.command.CommandInterface;
import com.codefollower.h2.command.ddl.DefineCommand;
import com.codefollower.h2.engine.Session;


public class CreateColumnFamily extends DefineCommand {

	private String cfName;
	private Options options;

	public CreateColumnFamily(Session session, String cfName) {
		super(session);
		this.cfName = cfName;
	}

	public String getColumnFamilyName() {
		return cfName;
	}

	public void setOptions(Options options) {
		this.options = options;
	}

	public int update() {
		return 0;
	}

	public int getType() {
		return CommandInterface.UNKNOWN;
	}

	public HColumnDescriptor createHColumnDescriptor() {
		HColumnDescriptor hcd = new HColumnDescriptor(cfName);
		if (options != null) {
			options.initOptions(hcd);
		}
		return hcd;
	}

}
