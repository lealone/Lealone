/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.misc.spsc;

import org.jctools.queues.SpscLinkedQueue;

public class JCToolsSpscLinkedQueueTest extends SpscTestBase {

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < LoopCount; i++)
            new JCToolsSpscLinkedQueueTest().run();
    }

    private final SpscLinkedQueue<AsyncTask> pendingTasks = new SpscLinkedQueue<>();

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
