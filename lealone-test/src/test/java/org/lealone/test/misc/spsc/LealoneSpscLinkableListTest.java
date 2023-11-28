/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.misc.spsc;

public class LealoneSpscLinkableListTest extends SpscTestBase {

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < LoopCount; i++)
            new LealoneSpscLinkableListTest().run();
    }

    // LinkableList是一个无锁且不需要CAS的普通链表，满足单生产者单消费者的应用场景
    private final LinkableList<PendingTask> pendingTasks = new LinkableList<>();

    @Override
    public void produce(AsyncTask task) {
        PendingTask pt = new PendingTask(task);
        pendingTasks.add(pt);
        if (pendingTasks.size() > 30000)
            removeCompletedTasks();
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

    @Override
    public void consume() {
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

    public class PendingTask {

        private final AsyncTask task;
        private boolean completed;

        private PendingTask next;

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

        public void setNext(PendingTask next) {
            this.next = next;
        }

        public PendingTask getNext() {
            return next;
        }
    }

    public class LinkableList<E extends PendingTask> {

        private E head;
        private E tail;
        private int size;

        public E getHead() {
            return head;
        }

        public void setHead(E head) {
            this.head = head;
        }

        public void setTail(E tail) {
            this.tail = tail;
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
    }
}
