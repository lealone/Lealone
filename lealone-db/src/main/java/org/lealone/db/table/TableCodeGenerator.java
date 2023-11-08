/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.table;

import org.lealone.db.Plugin;
import org.lealone.db.session.ServerSession;

public interface TableCodeGenerator extends Plugin {

    public void genCode(ServerSession session, Table table, Table owner, int level);

}
