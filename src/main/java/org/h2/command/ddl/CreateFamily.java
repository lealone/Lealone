package org.h2.command.ddl;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.h2.command.CommandInterface;
import org.h2.engine.Session;

public class CreateFamily extends DefineCommand {

	private String familyName;
	private boolean isDefault;
	private Options options;

	public CreateFamily(Session session) {
		super(session);
	}

	public String getFamilyName() {
		return familyName;
	}

	public void setFamilyName(String familyName) {
		this.familyName = familyName;
	}

	public boolean isDefault() {
		return isDefault;
	}

	public void setDefault(boolean isDefault) {
		this.isDefault = isDefault;
	}

	public Options getOptions() {
		return options;
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
		HColumnDescriptor hcd = new HColumnDescriptor(familyName);
		if (options != null) {
			options.initOptions(hcd);
		}

		hcd.setValue("IS_DEFAULT", isDefault ? "TRUE" : "FALSE");

		return hcd;
	}

}
