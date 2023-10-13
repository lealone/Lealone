/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aose;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

//这个类用于测试标记脏页和刷脏页算法的正确性，如果异常退出就说明算法有bug
public abstract class FlushDirtyPageTest {

    // 用CAS的方案要比synchronized好
    public static void main(String[] args) {
        run(new CASFlushDirtyPageTest());
        run(new SyncFlushDirtyPageTest());
    }

    public static void run(FlushDirtyPageTest test) {
        int count = 2000;
        long t1 = System.nanoTime();
        for (int i = 1; i <= count; i++) {
            if (i % 100 == 0 || i == count) {
                long t2 = System.nanoTime();
                System.out.println(test.getClass().getSimpleName() + " loop: " + i + ", time = "
                        + (t2 - t1) / 1000 / 1000 + " ms");
                t1 = t2;
            }
            test.run();
        }
    }

    protected final ConcurrentSkipListSet<Long> removedPages = new ConcurrentSkipListSet<>();
    protected final ConcurrentSkipListSet<Long> savedPages = new ConcurrentSkipListSet<>();

    protected void addRemovedPage(long pos) {
        removedPages.add(pos);
    }

    abstract void init();

    abstract void write(long pos);

    abstract void markDirty();

    abstract void replacePage();

    public void run() {
        init();
        removedPages.clear();
        savedPages.clear();

        // int count = 1;
        // int threadCount = 201;
        int count = 1000;
        int threadCount = 21;
        Thread[] threads = new Thread[threadCount];

        // 刷脏页线程，只有一个
        threads[0] = new Thread(() -> {
            for (int i = 1; i <= count; i++) {
                write(i);
            }
        });
        threads[0].setName("write");

        // 直接标记脏页的线程有多个
        for (int t = 1; t < (threadCount / 2) + 1; t++) {
            threads[t] = new Thread(() -> {
                for (int i = 0; i < count; i++) {
                    markDirty();
                }
            });
            threads[t].setName("markDirty-" + t);
        }

        // 通过复制新page把老page标记为脏页的线程也有多个
        for (int t = (threadCount / 2) + 1; t < threadCount; t++) {
            threads[t] = new Thread(() -> {
                for (int i = 0; i < count; i++) {
                    replacePage();
                }
            });
            threads[t].setName("replacePage-" + t);
        }

        for (int t = 0; t < threadCount; t++) {
            threads[t].start();
        }
        try {
            for (int t = 0; t < threadCount; t++) {
                threads[t].join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // System.out.println("savedPages size: " + savedPages.size());
        // System.out.println("removedPages size: " + removedPages.size());

        // 以下代码断言所有保存过的page都被标记为删除了，如果还有没被标记为删除的page，那说明算法还有bug
        markDirty();
        savedPages.removeAll(removedPages);
        if (!savedPages.isEmpty()) {
            System.out.println("error, saved pages size: " + savedPages.size());
            System.exit(-1);
        }
    }

    private static class CASFlushDirtyPageTest extends FlushDirtyPageTest {

        final PageReference ref = new PageReference();

        @Override
        void init() {
            Page page = new Page();
            // page.posRef.set(new PagePos(-1));
            ref.page = page;
        }

        @Override
        void write(long pos) {
            Page p = ref.getPage();
            PagePos oldPagePos = p.posRef.get();
            if (oldPagePos.v != 0) // 为0时才说明是脏页
                return;
            p.write();
            savedPages.add(pos);
            if (ref.getPage() != p) { // page复制过了
                addRemovedPage(pos);
            }
            if (!p.posRef.compareAndSet(oldPagePos, new PagePos(pos))) {
                addRemovedPage(pos);
            }
        }

        @Override
        void markDirty() {
            Page p = ref.getPage();
            p.markDirty();
        }

        @Override
        void replacePage() {
            Page p = new Page();
            ref.replacePage(p);
        }

        private class PagePos {
            final long v;

            PagePos(long v) {
                this.v = v;
            }
        }

        private class Page {

            final AtomicReference<PagePos> posRef = new AtomicReference<>(new PagePos(0));

            void markDirty() {
                PagePos old = posRef.get();
                if (posRef.compareAndSet(old, new PagePos(0))) {
                    if (old.v != 0) {
                        addRemovedPage(old.v);
                    }
                } else if (posRef.get().v != 0) { // 刷脏页线程刚写完，需要重试
                    markDirty();
                }
            }

            void write() {
            }
        }

        private static final AtomicReferenceFieldUpdater<PageReference, Page> //
        pageUpdater = AtomicReferenceFieldUpdater.newUpdater(PageReference.class, Page.class, "page");

        private class PageReference {

            volatile Page page;

            Page getPage() {
                return page;
            }

            void replacePage(Page newPage) {
                Page oldPage = this.page;
                if (oldPage == newPage)
                    return;
                if (pageUpdater.compareAndSet(this, oldPage, newPage)) {
                    if (oldPage != null)
                        oldPage.markDirty();
                }
            }

        }
    }

    private static class SyncFlushDirtyPageTest extends FlushDirtyPageTest {

        final Object lcok = new Object();
        final PageReference ref = new PageReference();

        @Override
        void init() {
            ref.page = new Page();
        }

        @Override
        void write(long pos) {
            synchronized (lcok) {
                Page p = ref.getPage();
                if (p.pos != 0) // 为0时才说明是脏页
                    return;
                p.write();
                savedPages.add(pos);
                p.pos = pos;
            }
        }

        @Override
        void markDirty() {
            Page p = ref.getPage();
            p.markDirty();
        }

        @Override
        void replacePage() {
            Page p = new Page();
            ref.replacePage(p);
        }

        private class Page {

            volatile long pos;

            void markDirty() {
                synchronized (lcok) {
                    if (pos != 0) {
                        addRemovedPage(pos);
                    }
                    pos = 0;
                }
            }

            void write() {
            }
        }

        private class PageReference {

            volatile Page page;

            Page getPage() {
                return page;
            }

            void replacePage(Page newPage) {
                synchronized (lcok) {
                    Page oldPage = this.page;
                    if (oldPage == newPage)
                        return;
                    this.page = newPage;
                    if (oldPage != null)
                        oldPage.markDirty();
                }
            }
        }
    }
}
