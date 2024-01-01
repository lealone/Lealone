/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

// 打印出漂亮的由page组成的btree
public class PrettyPagePrinter {

    public static void printPage(Page p) {
        printPage(p, true);
    }

    public static void printPage(Page p, boolean readOffLinePage) {
        System.out.println(getPrettyPageInfo(p, readOffLinePage));
    }

    public static String getPrettyPageInfo(Page p, boolean readOffLinePage) {
        StringBuilder buff = new StringBuilder();
        PrettyPageInfo info = new PrettyPageInfo();
        info.readOffLinePage = readOffLinePage;

        getPrettyPageInfoRecursive(p, "", info);

        buff.append("PrettyPageInfo:").append('\n');
        buff.append("--------------").append('\n');
        buff.append("pageCount: ").append(info.pageCount).append('\n');
        buff.append("leafPageCount: ").append(info.leafPageCount).append('\n');
        buff.append("nodePageCount: ").append(info.nodePageCount).append('\n');
        buff.append("levelCount: ").append(info.levelCount).append('\n');
        buff.append('\n');
        buff.append("pages:").append('\n');
        buff.append("--------------------------------").append('\n');

        buff.append(info.buff).append('\n');

        return buff.toString();
    }

    private static void getPrettyPageInfoRecursive(Page p, String indent, PrettyPageInfo info) {
        StringBuilder buff = info.buff;
        info.pageCount++;
        if (p.isNode())
            info.nodePageCount++;
        else
            info.leafPageCount++;
        int levelCount = indent.length() / 2 + 1;
        if (levelCount > info.levelCount)
            info.levelCount = levelCount;

        buff.append(indent).append("type: ").append(p.isLeaf() ? "leaf" : "node").append('\n');
        buff.append(indent).append("pos: ").append(p.getPos()).append('\n');
        buff.append(indent).append("chunkId: ").append(PageUtils.getPageChunkId(p.getPos()))
                .append('\n');
        // buff.append(indent).append("totalCount: ").append(p.getTotalCount()).append('\n');
        buff.append(indent).append("memory: ").append(p.getMemory()).append('\n');
        buff.append(indent).append("keyLength: ").append(p.getKeyCount()).append('\n');

        if (p.getKeyCount() > 0) {
            buff.append(indent).append("keys: ");
            for (int i = 0, len = p.getKeyCount(); i < len; i++) {
                if (i > 0)
                    buff.append(", ");
                buff.append(p.getKey(i));
            }
            buff.append('\n');
            if (p.isNode())
                getPrettyNodePageInfo(p, buff, indent, info);
            else
                getPrettyLeafPageInfo(p, buff, indent, info);
        }
    }

    private static void getPrettyNodePageInfo(Page p, StringBuilder buff, String indent,
            PrettyPageInfo info) {
        PageReference[] children = p.getChildren();
        if (children != null) {
            buff.append(indent).append("children: ").append(children.length).append('\n');
            for (int i = 0, len = children.length; i < len; i++) {
                buff.append('\n');
                if (children[i].getPage() != null) {
                    getPrettyPageInfoRecursive(children[i].getPage(), indent + "  ", info);
                } else {
                    if (info.readOffLinePage) {
                        Page child = children[i].getOrReadPage();
                        getPrettyPageInfoRecursive(child, indent + "  ", info);
                    } else {
                        buff.append(indent).append("  ");
                        buff.append("*** off-line *** ").append(children[i]).append('\n');
                    }
                }
            }
        }
    }

    private static void getPrettyLeafPageInfo(Page p, StringBuilder buff, String indent,
            PrettyPageInfo info) {
        buff.append(indent).append("values: ");
        for (int i = 0, len = p.getKeyCount(); i < len; i++) {
            if (i > 0)
                buff.append(", ");
            buff.append(p.getValue(i));
        }
        buff.append('\n');
    }

    private static class PrettyPageInfo {
        StringBuilder buff = new StringBuilder();
        int pageCount;
        int leafPageCount;
        int nodePageCount;
        int levelCount;
        boolean readOffLinePage;
    }
}
