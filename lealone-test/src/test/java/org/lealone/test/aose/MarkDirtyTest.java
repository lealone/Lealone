/// *
// * Copyright Lealone Database Group.
// * Licensed under the Server Side Public License, v 1.
// * Initial Developer: zhh
// */
// package org.lealone.test.aose;
//
// import java.util.Random;
// import java.util.concurrent.ConcurrentSkipListSet;
// import java.util.concurrent.atomic.AtomicReference;
//
// public class MarkDirtyTest {
//
// static int count = 1000;
//
// public static void main(String[] args) {
// MarkDirtyTest test = new MarkDirtyTest();
// for (int i = 0; i < count; i++) {
// test.run();
// }
// }
//
// public void run() {
// removedPages.clear();
// savedPages.clear();
// Page page = new Page();
// PageReference ref = new PageReference();
// ref.page = page;
// Random random = new Random();
//
// Thread t1 = new Thread(() -> {
// for (int i = 0; i < count; i++) {
// Page p = ref.getPage();
// PagePos oldPagePos = p.posRef.get();
// long pos = random.nextLong();
// savedPages.add(pos);
// if (ref.getPage() != p || !p.posRef.compareAndSet(oldPagePos, new PagePos(pos))) {
// addRemovedPage(pos);
// }
// }
// });
// Thread t2 = new Thread(() -> {
// for (int i = 0; i < count; i++) {
// Page p = ref.getPage();
// p.markDirty();
// }
// });
// Thread t3 = new Thread(() -> {
// for (int i = 0; i < count; i++) {
// Page p = new Page();
// ref.replacePage(p);
// }
// });
//
// t1.start();
// t2.start();
// t3.start();
// try {
// t1.join();
// t2.join();
// t3.join();
// } catch (InterruptedException e) {
// e.printStackTrace();
// }
// ref.getPage().markDirty();
// savedPages.removeAll(removedPages);
// if (!savedPages.isEmpty())
// System.out.println("savedPages size: " + savedPages.size());
// }
//
// private final ConcurrentSkipListSet<Long> removedPages = new ConcurrentSkipListSet<>();
// private final ConcurrentSkipListSet<Long> savedPages = new ConcurrentSkipListSet<>();
//
// private void addRemovedPage(long pos) {
// removedPages.add(pos);
// }
//
// private static class PagePos {
// final long v;
//
// PagePos(long v) {
// this.v = v;
// }
// }
//
// private class Page {
//
// final AtomicReference<PagePos> posRef = new AtomicReference<>(new PagePos(0));
//
// void markDirty() {
// PagePos old = posRef.get();
// if (posRef.compareAndSet(old, new PagePos(0))) {
// if (old.v != 0) {
// addRemovedPage(old.v);
// }
// } else if (posRef.get().v != 0) { // 刷脏页线程刚写完，需要重试
// markDirty();
// }
// }
// }
//
// private class PageReference {
//
// volatile Page page;
//
// Page getPage() {
// return page;
// }
//
// void replacePage(Page newPage) {
// Page oldPage = this.page;
// if (oldPage == newPage)
// return;
// this.page = newPage;
// if (oldPage != null)
// oldPage.markDirty();
// if (newPage != null)
// newPage.markDirty();
// }
// }
// }
