/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aose;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;

//这个类用于测试标记脏页和刷脏页算法的正确性，如果异常退出就说明算法有bug
public class FlushDirtyPageTest {

    static int count = 20000;

    public static void main(String[] args) {
        FlushDirtyPageTest test = new FlushDirtyPageTest();
        for (int i = 1; i <= count; i++) {
            if (i == 1 || i % 100 == 0 || i == count)
                System.out.println("loop: " + i);
            test.run();
        }
    }

    public void run() {
        removedPages.clear();
        savedPages.clear();
        Page page = new Page();
        // page.posRef.set(new PagePos(-1));
        PageReference ref = new PageReference();
        ref.page = page;

        // int count = 1;
        // int threadCount = 201;
        int count = 1000;
        int threadCount = 21;
        Thread[] threads = new Thread[threadCount];

        // 刷脏页线程，只有一个
        threads[0] = new Thread(() -> {
            for (int i = 1; i <= count; i++) {
                Page p = ref.getPage();
                PagePos oldPagePos = p.posRef.get();
                if (oldPagePos.v != 0) // 为0时才说明是脏页
                    continue;
                p.write();
                long pos = i;
                savedPages.add(pos);
                if (ref.getPage() != p) { // page复制过了
                    addRemovedPage(pos);
                }
                if (!p.posRef.compareAndSet(oldPagePos, new PagePos(pos))) {
                    addRemovedPage(pos);
                }
            }
        });
        threads[0].setName("write");

        // 直接标记脏页的线程有多个
        for (int t = 1; t < (threadCount / 2) + 1; t++) {
            threads[t] = new Thread(() -> {
                for (int i = 0; i < count; i++) {
                    Page p = ref.getPage();
                    p.markDirty();
                }
            });
            threads[t].setName("markDirty-" + t);
        }

        // 通过复制新page把老page标记为脏页的线程也有多个
        for (int t = (threadCount / 2) + 1; t < threadCount; t++) {
            threads[t] = new Thread(() -> {
                for (int i = 0; i < count; i++) {
                    Page p = new Page();
                    ref.replacePage(p);
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
        ref.getPage().markDirty();
        savedPages.removeAll(removedPages);
        if (!savedPages.isEmpty()) {
            System.out.println("error, savedPages size: " + savedPages.size());
            System.exit(-1);
        }
    }

    private final ConcurrentSkipListSet<Long> removedPages = new ConcurrentSkipListSet<>();
    private final ConcurrentSkipListSet<Long> savedPages = new ConcurrentSkipListSet<>();

    private void addRemovedPage(long pos) {
        removedPages.add(pos);
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

    private class PageReference {

        volatile Page page;

        Page getPage() {
            return page;
        }

        void replacePage(Page newPage) {
            Page oldPage = this.page;
            if (oldPage == newPage)
                return;
            this.page = newPage;
            if (oldPage != null)
                oldPage.markDirty();
            if (newPage != null)
                newPage.markDirty();
        }
    }
}
