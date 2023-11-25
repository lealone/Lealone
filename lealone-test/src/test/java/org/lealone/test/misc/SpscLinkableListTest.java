/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.misc;

import java.util.concurrent.CountDownLatch;

public class SpscLinkableListTest {

    public static void main(String[] args) throws Exception {
        // for (int i = 0; i < 100; i++)
        new SpscLinkableListTest().run();
    }

    // LinkableList是一个无锁且不需要CAS的普通链表，满足单生产者单消费者的应用场景
    private final LinkableList<PendingTask> pendingTasks = new LinkableList<>();
    private final long pendingTaskCount = 100 * 10000; // 待处理任务总数
    private long completedTaskCount; // 已经完成的任务数

    private long result; // 存放计算结果

    private void run() throws Exception {
        // 生产者创建pendingTaskCount个AsyncTask
        // 每个AsyncTask的工作就是计算从1到pendingTaskCount的和
        Thread producer = new Thread(() -> {
            for (int i = 1; i <= pendingTaskCount; i++) {
                AsyncTask task = new AsyncTask(i);
                submitTask(task);
            }
        });

        // 消费者不断从pendingTasks中取出AsyncTask执行
        Thread consumer = new Thread(() -> {
            while (completedTaskCount < pendingTaskCount) {
                runPendingTasks();
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
            System.out.println("result: " + result + ", ok, cost " + t + "ms");
        } else {
            System.out.println("result: " + result + ", not ok, except: " + except);
        }
    }

    private void submitTask(AsyncTask task) {
        if (pendingTasks.size() > 1000) {
            CountDownLatch latch = new CountDownLatch(1);
            task.latch = latch;
            PendingTask pt = new PendingTask(task);
            pendingTasks.add(pt);
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (pendingTasks.size() > 1)
                removeCompletedTasks();
        } else {
            PendingTask pt = new PendingTask(task);
            pendingTasks.add(pt);
            if (pendingTasks.size() > 1)
                removeCompletedTasks();
        }
    }

    private void removeCompletedTasks() {
        PendingTask pt = pendingTasks.getHead();
        while (pt != null && pt.isCompleted()) {
            pt = pt.getNext();
            pendingTasks.decrementSize();
            pendingTasks.setHead(pt);
        }
        if (pendingTasks.getHead() == null)
            pendingTasks.setTail(null);
    }

    private void runPendingTasks() {
        PendingTask pt = pendingTasks.getHead();
        while (pt != null) {
            if (!pt.isCompleted()) {
                completedTaskCount++;
                pt.getTask().compute();
                pt.setCompleted(true);
            }
            pt = pt.getNext();
        }
    }

    public class AsyncTask {
        int value;
        CountDownLatch latch;

        AsyncTask(int value) {
            this.value = value;
        }

        void compute() {
            result += value;
            if (latch != null)
                latch.countDown();
        }
    }

    public class PendingTask extends LinkableBase<PendingTask> {

        private final AsyncTask task;
        private boolean completed;

        public PendingTask(AsyncTask task) {
            this.task = task;
        }

        public AsyncTask getTask() {
            return task;
        }

        public boolean isCompleted() {
            return completed;
        }

        public void setCompleted(boolean completed) {
            this.completed = completed;
        }
    }

    public interface Linkable<E extends Linkable<E>> {

        void setNext(E next);

        E getNext();

    }

    public class LinkableBase<E extends Linkable<E>> implements Linkable<E> {

        public E next;

        @Override
        public void setNext(E next) {
            this.next = next;
        }

        @Override
        public E getNext() {
            return next;
        }
    }

    public class LinkableList<E extends Linkable<E>> {

        private E head;
        private E tail;
        private int size;

        public E getHead() {
            return head;
        }

        public void setHead(E head) {
            this.head = head;
        }

        public E getTail() {
            return tail;
        }

        public void setTail(E tail) {
            this.tail = tail;
        }

        public boolean isEmpty() {
            return head == null;
        }

        public int size() {
            return size;
        }

        public void decrementSize() {
            size--;
        }

        public void add(E e) {
            size++;
            if (head == null) {
                head = tail = e;
            } else {
                tail.setNext(e);
                tail = e;
            }
        }

        public void remove(E e) {
            size--;
            if (head == e) { // 删除头
                head = e.getNext();
                if (head == null)
                    tail = null;
            } else {
                E n = head;
                E last = n;
                while (n != null) {
                    if (e == n) {
                        last.setNext(n.getNext());
                        break;
                    }
                    last = n;
                    n = n.getNext();
                }
                if (tail == e) // 删除尾
                    tail = last;
            }
        }
    }
}
