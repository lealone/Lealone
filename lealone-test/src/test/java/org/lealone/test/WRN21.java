/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

/**
 * 
 * W=R=N/2+1算法模拟，
 * 重点模拟update操作，没有insert和delete，
 * Node代表集群中的一个节点，
 * 为了简化，每个Node只有一条记录。
 * 
 */
public class WRN21 {

    static class Record {
        int version; // 每次更新时自动加1
        int value;

        // 每次修改记录的事务名要全局唯一，
        // 比如用节点的IP拼接一个本地递增的计数器组成字符串就足够了
        String transactionName;

        @Override
        public String toString() {
            return "Record[" + version + ", " + value + ", " + transactionName + "]";
        }
    }

    static class Node {
        private final String name; // 节点名
        private ArrayList<Record> historyRecords = new ArrayList<>(); // 保存记录的历史修改版本

        public Node(String name, Record originalRecord) {
            this.name = name;
            historyRecords.add(originalRecord);
        }

        // 可以把update方法实现成通过网络的方式更新记录
        public synchronized ArrayList<String> update(String transactionName, int v) {
            Record newRecord = new Record();
            Record last = historyRecords.get(historyRecords.size() - 1);
            newRecord.version = last.version + 1;
            newRecord.value = last.value + v;
            newRecord.transactionName = transactionName;

            // 这里可以优化，比如加一个安全检查点，在此检查点之前的历史修改版本都可以安全删除了
            ArrayList<String> list = new ArrayList<>(historyRecords.size());
            for (int i = 0; i < historyRecords.size(); i++) {
                list.add(historyRecords.get(i).transactionName + "$" + historyRecords.get(i).version);
            }
            historyRecords.add(newRecord);
            return list;
        }

        // 可以把 rollback方法实现成通过网络的方式撤销更新
        // 注意，并没有对应的commit方法
        public synchronized void rollback(String transactionName) {
            int index = -1;
            for (int i = 0; i < historyRecords.size(); i++) {
                if (historyRecords.get(i).transactionName.equals(transactionName)) {
                    index = i;
                    break;
                }
            }

            if (index != -1) {
                ArrayList<Record> list = new ArrayList<>();
                for (int i = 0; i < index; i++) {
                    list.add(historyRecords.get(i));
                }
                historyRecords = list;
            }
        }

        // 为什么这样写就会出错呢
        public synchronized void rollback_wrong(String transactionName) {
            int index = -1;
            for (int i = 0; i < historyRecords.size(); i++) {
                if (historyRecords.get(i).transactionName.equals(transactionName)) {
                    historyRecords.remove(i);
                    index = i;
                    break;
                }
            }
            if (index != -1) {
                // 因为这里会少删除一些记录
                for (int i = index; i < historyRecords.size(); i++) {
                    historyRecords.remove(i);
                }
            }
        }

        public synchronized Record getRecord() {
            return historyRecords.get(historyRecords.size() - 1);
        }

        @Override
        public synchronized String toString() {
            return name + historyRecords;
        }
    }

    static class WThread extends Thread {
        int loopCount;
        int transactionId;

        public WThread(String name, int loopCount) {
            super(name);
            this.loopCount = loopCount;
        }

        @Override
        public void run() {
            while (loopCount > 0) {
                int conflictCount = 0;
                while (true) {
                    if (update()) {
                        break;
                    } else {
                        conflictCount++;
                    }
                }
                if (conflictCount > 10000)
                    System.out.println(getName() + " conflict " + conflictCount + " times");
                loopCount--;
            }
        }

        boolean update() {
            String transactionName = getName() + "-" + transactionId++;
            if (transactionId > Integer.MAX_VALUE - 1) {
                System.out.println("transactionId overflow......");
                System.exit(-1);
            }

            int n = nodes.length;
            int w = n / 2 + 1;
            Node[] w_nodes = new Node[w];
            HashSet<Node> seen = new HashSet<>();
            // 每个节点上历史记录的事务名和版本号组成的字符串
            ArrayList<ArrayList<String>> historyRecordInfo = new ArrayList<>();
            for (int i = 0; i < w; i++) {
                Node node = getRandomNode(seen, n);
                historyRecordInfo.add(node.update(transactionName, 1)); // 每次加１
                w_nodes[i] = node;
            }

            // 检查冲突，同时检查是否写入的是一个过期的节点
            boolean conflict = false;
            outerLoop: for (int i = 0; i < w - 1; i++) {
                ArrayList<String> a = historyRecordInfo.get(i);
                ArrayList<String> b = historyRecordInfo.get(i + 1);
                // 1.不同节点上的历史记录必须具有相同的高度(竖起来看)
                if (a.size() != b.size()) {
                    conflict = true;
                    break;
                }
                int size = a.size();
                for (int j = 0; j < size; j++) {
                    // 2.不同节点中同一级历史记录项(水平切分)必须有相同的事务名和版本号
                    if (!a.get(j).equals(b.get(j))) {
                        conflict = true;
                        break outerLoop;
                    }
                }
            }
            if (conflict) {
                for (int i = 0; i < w; i++) {
                    w_nodes[i].rollback(transactionName);
                }
            }
            return !conflict;
        }
    }

    static Node[] nodes;
    static Random random = new Random();

    public static void main(String[] args) throws Exception {
        int replicas = 3;
        if (args.length > 0)
            replicas = Integer.parseInt(args[0]);
        System.out.println("replicas: " + replicas);

        int count = 1000;
        while (count-- > 0) {
            int initValue = 100;
            nodes = new Node[replicas];
            for (int i = 0; i < replicas; i++) {
                Record r = new Record();
                r.value = initValue;
                r.version = 10;
                r.transactionName = "main";

                nodes[i] = new Node("N" + (i + 1), r);
            }

            int threadCount = 10;
            int loopCount = 5;
            WThread[] threads = new WThread[threadCount];
            for (int i = 0; i < threadCount; i++) {
                threads[i] = new WThread("WThread-" + i, loopCount);
            }
            for (int i = 0; i < threadCount; i++) {
                threads[i].start();
            }
            for (int i = 0; i < threadCount; i++) {
                threads[i].join();
            }

            int n = nodes.length;
            int r = n / 2 + 1;
            Record[] records = new Record[r];
            HashSet<Node> seen = new HashSet<>();
            for (int i = 0; i < r; i++) {
                Node node = getRandomNode(seen, n);
                records[i] = node.getRecord();
            }

            Record record = records[0];
            for (int i = 1; i < r; i++) {
                if (records[i].version > record.version) {
                    record = records[i];
                }
            }

            // 每个线程在每次循环中对记录加1，
            // 所以记录的最终值就是: initValue + threadCount * loopCount
            // 如果不相等，说明算法实现不正确
            if (record.value != initValue + threadCount * loopCount) {
                System.out.println();
                System.out.println("Fatal: " + record);
                printNodes();
                break;
            }

            System.out.println("Ok: " + record);
        }
    }

    static Node getRandomNode(HashSet<Node> seen, int n) {
        while (true) {
            Node node = nodes[random.nextInt(n)]; // 随机选择一个节点，但是不能跟前面选过的重复
            if (seen.add(node)) {
                return node;
            }
        }
    }

    static void printNodes() {
        for (Node n : nodes)
            System.out.print(n + " ");
        System.out.println();
    }
}
