/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.lealone.db.ConnectionSetting;

public class AsyncConnectionPool {

    private final CopyOnWriteArrayList<AsyncConnection> list = new CopyOnWriteArrayList<>();

    public AsyncConnection getConnection(Map<String, String> config) {
        if (!isShared(config))
            return null;
        AsyncConnection best = null;
        int min = Integer.MAX_VALUE;
        for (AsyncConnection c : list) {
            int maxSharedSize = c.getMaxSharedSize();
            if (maxSharedSize == 1)
                continue;
            int size = c.getSharedSize();
            if (maxSharedSize > 0 && size >= maxSharedSize)
                continue;
            if (size < min) {
                best = c;
                min = size;
            }
        }
        return best;
    }

    public void addConnection(AsyncConnection conn) {
        list.add(conn);
    }

    public void removeConnection(AsyncConnection conn) {
        list.remove(conn);
    }

    public void close() {
        for (AsyncConnection c : list) {
            try {
                c.close();
            } catch (Throwable e) {
            }
        }
        list.clear();
    }

    public void checkTimeout(long currentTime) {
        for (AsyncConnection c : list) {
            c.checkTimeout(currentTime);
        }
    }

    public static int getMaxSharedSize(Map<String, String> config) {
        if (!isShared(config))
            return 1; // 独享受模式
        int maxSharedSize;
        if (config.containsKey(ConnectionSetting.MAX_SHARED_SIZE.name()))
            maxSharedSize = Integer.parseInt(config.get(ConnectionSetting.MAX_SHARED_SIZE.name()));
        else
            maxSharedSize = -1;
        return maxSharedSize;
    }

    public static boolean isShared(Map<String, String> config) {
        String str = config.get(ConnectionSetting.IS_SHARED.name());
        // 为null时默认是共享模式
        return str == null || Boolean.parseBoolean(str);
    }
}
