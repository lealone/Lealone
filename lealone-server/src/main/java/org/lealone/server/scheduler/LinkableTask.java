/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.scheduler;

import org.lealone.db.async.AsyncTask;
import org.lealone.db.link.LinkableBase;

public abstract class LinkableTask extends LinkableBase<LinkableTask> implements AsyncTask {
}
