/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.scheduler;

import com.lealone.db.async.AsyncTask;
import com.lealone.db.link.LinkableBase;

public abstract class LinkableTask extends LinkableBase<LinkableTask> implements AsyncTask {
}
