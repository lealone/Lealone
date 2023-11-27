/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.misc.spsc;

public abstract class SpscTestBase {

    public static final long LoopCount = 10000;

    protected final long pendingTaskCount = 1000 * 10000; // 待处理任务总数
    protected long completedTaskCount; // 已经完成的任务数
    protected long result; // 存放计算结果

    public void run() throws Exception {
        // 生产者创建pendingTaskCount个AsyncTask
        // 每个AsyncTask的工作就是计算从1到pendingTaskCount的和
        Thread producer = new Thread(() -> {
            for (int i = 1; i <= pendingTaskCount; i++) {
                AsyncTask task = new AsyncTask(i);
                produce(task);
            }
        });

        // 消费者不断从pendingTasks中取出AsyncTask执行
        Thread consumer = new Thread(() -> {
            while (completedTaskCount < pendingTaskCount) {
                consume();
            }
        });

        long t = System.currentTimeMillis();
        producer.start();
        consumer.start();
        producer.join();
        consumer.join();
        t = System.currentTimeMillis() - t;

        // 如果result跟except相同，说明代码是ok的，如果不同，那就说明代码有bug
        long except = (1 + pendingTaskCount) * pendingTaskCount / 2;
        if (result == except) {
            System.out.println(
                    getClass().getSimpleName() + " result: " + result + ", ok, cost " + t + " ms");
        } else {
            System.out.println(
                    getClass().getSimpleName() + "result: " + result + ", not ok, except: " + except);
        }
    }

    public abstract void produce(AsyncTask task);

    public abstract void consume();

    public class AsyncTask {

        private final int value;

        public AsyncTask(int value) {
            this.value = value;
        }

        public void compute() {
            result += value;
        }
    }
}
