/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.lealone.common.util.MapUtils;
import com.lealone.db.ConnectionSetting;

public class AsyncConnectionPool {

    private static final AtomicInteger exclusiveSize = new AtomicInteger();
    private static int maxExclusiveSize;

    public static boolean isExceededMaxExclusiveSize() {
        return exclusiveSize.get() > maxExclusiveSize;
    }

    public static void incrementExclusiveSize() {
        exclusiveSize.incrementAndGet();
    }

    public static void decrementExclusiveSize() {
        exclusiveSize.decrementAndGet();
    }

    public static int getMaxExclusiveSize() {
        return maxExclusiveSize;
    }

    public static void setMaxExclusiveSize(int maxExclusiveSize) {
        AsyncConnectionPool.maxExclusiveSize = maxExclusiveSize;
    }

    public static void setMaxExclusiveSize(Map<String, String> config) {
        AsyncConnectionPool.maxExclusiveSize = MapUtils.getInt(config,
                ConnectionSetting.MAX_EXCLUSIVE_SIZE.name(),
                Runtime.getRuntime().availableProcessors() * 10);
    }

    private final List<AsyncConnection> list = new ArrayList<>();

    // 直接取第一条
    public AsyncConnection getConnection() {
        if (list == null || list.isEmpty()) {
            return null;
        } else {
            return list.get(0);
        }
    }

    public AsyncConnection getConnection(Map<String, String> config) {
        if (!isShared(config)) {
            // 专用连接如果空闲了也可以直接复用
            for (AsyncConnection c : list) {
                if (c.getMaxSharedSize() == 1 && c.getSharedSize() == 0)
                    return c;
            }
            return null;
        }
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
        if (!conn.isShared())
            incrementExclusiveSize();
        list.add(conn);
    }

    public void removeConnection(AsyncConnection conn) {
        if (!conn.isShared())
            decrementExclusiveSize();
        list.remove(conn);
    }

    public boolean isEmpty() {
        return list.isEmpty();
    }

    public void close() {
        for (AsyncConnection c : list) {
            if (!c.isShared())
                decrementExclusiveSize();
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
        if (isShared(config))
            return MapUtils.getInt(config, ConnectionSetting.MAX_SHARED_SIZE.name(), 3);
        else
            return 1; // 独享模式
    }

    public static boolean isShared(Map<String, String> config) {
        // 为null时默认是独享模式
        return MapUtils.getBoolean(config, ConnectionSetting.IS_SHARED.name(), false);
    }
}
