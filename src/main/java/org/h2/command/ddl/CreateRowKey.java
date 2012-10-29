package org.h2.command.ddl;

import org.h2.command.CommandInterface;
import org.h2.engine.Session;

public class CreateRowKey extends DefineCommand {

	private String rowKeyName;

	public CreateRowKey(Session session, String rowKeyName) {
		super(session);
		this.rowKeyName = rowKeyName;
	}

	public String getRowKeyName() {
		return rowKeyName;
	}

	public int update() {
		return 0;
	}

	public int getType() {
		return CommandInterface.UNKNOWN;
	}

}
