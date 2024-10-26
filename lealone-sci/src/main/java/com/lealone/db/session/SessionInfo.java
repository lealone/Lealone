/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.session;

import com.lealone.db.async.AsyncTask;

public interface SessionInfo {

    Session getSession();

    int getSessionId();

    void submitTask(AsyncTask task);

}
