/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.misc.spsc;

import java.util.concurrent.ConcurrentLinkedQueue;

public class JdkConcurrentLinkedQueueTest extends SpscTestBase {

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < LoopCount; i++)
            new JdkConcurrentLinkedQueueTest().run();
    }

    private final ConcurrentLinkedQueue<AsyncTask> pendingTasks = new ConcurrentLinkedQueue<>();

    @Override
    public void produce(AsyncTask task) {
        pendingTasks.offer(task);
    }

    @Override
    public void consume() {
        AsyncTask task = pendingTasks.poll();
        if (task != null) {
            task.compute();
            completedTaskCount++;
        }
    }
}
