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
package org.lealone.test.misc;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.lealone.common.util.DataUtils;

//试验性的算法
//对于long类型的page key，如果传输上百万个，是否可以进行适当编码减少传输大小
public class PageKeyCodec {

    static class Xcoordinate {
        BitField bf;
        List<Integer> xList;
    }

    public static void main(String[] args) {
        long maxValue = 10_000_000_000L;

        TreeSet<Long> values = new TreeSet<>();
        // Random random = new Random();
        // random.setSeed(maxValue);
        int valueCount = 100 * 10000;
        // valueCount = 100000;
        // valueCount = 60 * 10000;
        // valueCount = 1 * 10000;
        valueCount = 400 * 1000;
        long randomMaxValue = -1;
        // for (int i = 0; i < valueCount; i++) {
        // long value = random.nextInt((int) maxValue);
        // if (value > randomMaxValue)
        // randomMaxValue = value;
        // values.add(value);
        // }

        for (int i = 0; i < valueCount * 2; i++) {
            long value = i;
            values.add((long) i);
            if (value > randomMaxValue)
                randomMaxValue = value;
        }
        // maxValue = randomMaxValue;

        double sqrt = Math.sqrt(maxValue);
        int longFieldCount = getLongFielintdCount(sqrt);

        int maxLongFieldCount = longFieldCount;

        long bitCount = longFieldCount * 64;
        long[] datax = new long[longFieldCount];
        long[] datay = new long[longFieldCount];
        BitField bfx = new BitField(datax);
        BitField bfy = new BitField(datay);

        BitField[] bfxy = new BitField[longFieldCount];
        for (int i = 0; i < longFieldCount; i++) {
            long[] data = new long[longFieldCount];
            bfxy[i] = new BitField(data);
        }

        // System.out.println("values 1: " + values);

        TreeMap<Integer, List<Integer>> y2xMap = new TreeMap<>();

        TreeSet<Integer> xValues = new TreeSet<>();

        int maxY = -1;
        for (long value : values) {
            int x = (int) (value % bitCount);
            int y = (int) (value / bitCount);
            // bfx.set(x);
            // bfx.set(y);
            List<Integer> xList = y2xMap.get(y);
            if (xList == null) {
                xList = new ArrayList<>();
                xList.add(x);
                y2xMap.put(y, xList);
            }
            int maxX = xList.get(0);
            xList.add(x);
            if (y > maxY) {
                maxY = y;
            }
            if (x > maxX) {
                maxX = x;
            }
            xList.set(0, maxX);
            xValues.add(x);

            y = y % longFieldCount;
            bfxy[y].set(x);

            // long v = y * bitCount + x;
            // System.out.println("value: " + value + ", v: " + v);
        }
        System.out.println("y2xMap keys: " + y2xMap.keySet());
        // System.out.println("y2xMap: " + y2xMap);
        System.out.println("values.size: " + values.size() + ", y2xMap.size: " + y2xMap.size());
        values = new TreeSet<>();
        for (Map.Entry<Integer, List<Integer>> e : y2xMap.entrySet()) {
            int y = e.getKey();
            List<Integer> xList = e.getValue();
            for (int i = 1, size = xList.size(); i < size; i++) {
                int x = xList.get(i);
                long v = y * bitCount + x;
                values.add(v);
            }
        }
        System.out.println("values.size: " + values.size());
        // System.out.println("values 1: " + values);

        longFieldCount = getLongFielintdCount(maxY);
        datay = new long[longFieldCount];
        bfy = new BitField(datay);
        ArrayList<BitField> bfxList = new ArrayList<>(y2xMap.size());
        ArrayList<Xcoordinate> xCoordinateList = new ArrayList<>(y2xMap.size());
        for (Map.Entry<Integer, List<Integer>> e : y2xMap.entrySet()) {
            int y = e.getKey();
            bfy.set(y);
            List<Integer> xList = e.getValue();
            int maxX = xList.get(0);
            longFieldCount = maxX / 64;
            if (maxX % 64 != 0) {
                longFieldCount++;
            }

            Xcoordinate xCoordinate = new Xcoordinate();
            xCoordinateList.add(xCoordinate);
            int xCount = xList.size() - 1;
            if (xCount < maxLongFieldCount || longFieldCount > maxLongFieldCount) {
                xCoordinate.xList = xList.subList(1, xList.size());
                continue;
            }
            longFieldCount = getLongFielintdCount(maxX);
            datax = new long[longFieldCount];
            bfx = new BitField(datax);
            for (int i = 1, size = xList.size(); i < size; i++) {
                int x = xList.get(i);
                bfx.set(x);
            }
            xCoordinate.bf = bfx;
            bfxList.add(bfx);
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(5 * 1024 * 1024);
        // 如果long很大，writeVarLong可能会写到9个字节
        for (long y : bfy.getLongFields()) {
            DataUtils.writeVarLong(byteBuffer, y);
            // byteBuffer.putLong(y);
        }

        for (Xcoordinate xCoordinate : xCoordinateList) {
            if (xCoordinate.xList != null) {
                for (int x : xCoordinate.xList) {
                    DataUtils.writeVarLong(byteBuffer, x);
                }
            } else {
                bfx = xCoordinate.bf;
                for (long x : bfx.getLongFields()) {
                    DataUtils.writeVarLong(byteBuffer, x);
                }
            }
        }
        byteBuffer.flip();
        System.out.println("byteBuffer bytes new1: " + byteBuffer.limit());

        // 压缩效果不明显
        // byte[] dst = new byte[byteBuffer.limit() * 2];
        // Compressor compressor = new CompressLZF();
        // int lencompress = compressor.compress(byteBuffer.array(), byteBuffer.limit(), dst, 0);
        // System.out.println("compress bytes new1: " + lencompress);
        byteBuffer = ByteBuffer.allocate(5 * 1024 * 1024);
        for (long v : values) {
            DataUtils.writeVarLong(byteBuffer, v);
        }
        byteBuffer.flip();
        System.out.println("byteBuffer bytes old1: " + byteBuffer.limit());

        int validCount = 0;
        byteBuffer = ByteBuffer.allocate(5 * 1024 * 1024);
        for (int i = 0; i < bfxy.length; i++) {
            bfx = bfxy[i];
            boolean isValid = false;
            long[] fields = bfx.getLongFields();
            for (long x : fields) {
                if (x > 0) {
                    isValid = true;
                    break;
                }
            }
            if (isValid) {
                DataUtils.writeVarInt(byteBuffer, i + 1);
            }
            for (int j = 0; j < fields.length; j++) {
                if (fields[j] > 0) {
                    validCount++;
                    DataUtils.writeVarInt(byteBuffer, j + 1);
                    DataUtils.writeVarLong(byteBuffer, fields[j]);
                }
            }
            // for (long x : bfx.getLongFields()) {
            // // int y = i;
            // // long v = y * bitCount + x;
            // DataUtils.writeVarLong(byteBuffer, x);
            // // byteBuffer.putLong(x);
            // if (x > 0)
            // validCount++;
            // }
        }
        byteBuffer.flip();
        System.out.println("byteBuffer bytes new2: " + byteBuffer.limit());
        System.out.println("validCount: " + validCount);
        int bfyLength = 0;
        for (int i = 0, len = bfy.getBitLength(); i < len; i++) {
            if (bfy.get(i)) {
                bfyLength++;
            }
        }
        System.out.println(bfy.length() + ", bfyLength: " + bfyLength);
        System.out.println(bfxList.size());

        int newLongFieldCount = bfy.getLongFieldCount();
        int bfxLongFieldCount = 0;
        TreeSet<Long> values2 = new TreeSet<>();
        int bfxListIndex = 0;
        for (int i = 1, len = bfy.getBitLength(); i <= len; i++) {
            if (bfy.get(i)) {
                int y = i;
                Xcoordinate xCoordinate = xCoordinateList.get(bfxListIndex);
                if (xCoordinate.xList != null) {
                    for (int x : xCoordinate.xList) {
                        long v = y * bitCount + x;
                        values2.add(v);
                    }
                    newLongFieldCount += xCoordinate.xList.size();
                    bfxLongFieldCount += xCoordinate.xList.size();
                } else {
                    bfx = xCoordinate.bf;
                    newLongFieldCount += bfx.getLongFieldCount();
                    bfxLongFieldCount += bfx.getLongFieldCount();
                    for (int j = 1, jlen = bfx.getBitLength(); j <= jlen; j++) {
                        if (bfx.get(j)) {
                            int x = j;
                            long v = y * bitCount + x;
                            values2.add(v);
                        }
                    }
                }
                bfxListIndex++;
            }
        }
        // System.out.println("values 2: " + values2);
        System.out.println("bfy longFieldCount: " + bfy.getLongFieldCount());
        System.out.println("bfx longFieldCount: " + bfxLongFieldCount);
        System.out.println("newLongFieldCount: " + newLongFieldCount);
        System.out.println("xValues size: " + xValues.size());
    }

    static int getLongFielintdCount(long maxValue) {
        long longFieldCount = maxValue / 64;
        if (maxValue % 64 != 0) {
            longFieldCount++;
        }
        return (int) longFieldCount;
    }

    static int getLongFielintdCount(double maxValue) {
        long longFieldCount = (long) (maxValue / 64);
        if (maxValue % 64 != 0) {
            longFieldCount++;
        }
        return (int) longFieldCount;
    }

    final static class BitField {

        private static final int ADDRESS_BITS = 6;
        private static final int BITS = 64;
        private static final int ADDRESS_MASK = BITS - 1;
        private long[] data;
        private int maxLength;
        HashMap<Integer, Integer> bitRepeatCount = new HashMap<>();

        public BitField() {
            this(64);
        }

        public BitField(long[] data) {
            this.data = data;
        }

        public BitField(int capacity) {
            data = new long[capacity >>> 3];
        }

        public int getRepeatCount(int index) {
            Integer repeatCount = bitRepeatCount.get(index);
            if (repeatCount == null) {
                return 0;
            } else {
                return repeatCount.intValue();
            }
        }

        public int getBitLength() {
            return data.length * 64;
        }

        public long[] getLongFields() {
            return data;
        }

        public int getLongFieldCount() {
            return data.length;
        }

        /**
         * Get the index of the next bit that is not set.
         *
         * @param fromIndex where to start searching
         * @return the index of the next disabled bit
         */
        public int nextClearBit(int fromIndex) {
            int i = fromIndex >> ADDRESS_BITS;
            int max = data.length;
            for (; i < max; i++) {
                if (data[i] == -1) {
                    continue;
                }
                int j = Math.max(fromIndex, i << ADDRESS_BITS);
                for (int end = j + 64; j < end; j++) {
                    if (!get(j)) {
                        return j;
                    }
                }
            }
            return max << ADDRESS_BITS;
        }

        /**
         * Get the bit at the given index.
         *
         * @param i the index
         * @return true if the bit is enabled
         */
        public boolean get(int i) {
            int addr = i >> ADDRESS_BITS;
            if (addr >= data.length) {
                return false;
            }
            return (data[addr] & getBitMask(i)) != 0;
        }

        /**
         * Get the next 8 bits at the given index.
         * The index must be a multiple of 8.
         *
         * @param i the index
         * @return the next 8 bits
         */
        public int getByte(int i) {
            int addr = i >> ADDRESS_BITS;
            if (addr >= data.length) {
                return 0;
            }
            return (int) (data[addr] >>> (i & (7 << 3)) & 255);
        }

        /**
         * Combine the next 8 bits at the given index with OR.
         * The index must be a multiple of 8.
         *
         * @param i the index
         * @param x the next 8 bits (0 - 255)
         */
        public void setByte(int i, int x) {
            int addr = i >> ADDRESS_BITS;
            checkCapacity(addr);
            data[addr] |= ((long) x) << (i & (7 << 3));
            if (maxLength < i && x != 0) {
                maxLength = i + 7;
            }
        }

        /**
         * Set bit at the given index to 'true'.
         *
         * @param i the index
         */
        public void set(int i) {
            if (get(i)) {
                Integer repeatCount = bitRepeatCount.get(i);
                if (repeatCount == null) {
                    repeatCount = 2;
                } else {
                    repeatCount++;
                }
                bitRepeatCount.put(i, repeatCount);
            }
            int addr = i >> ADDRESS_BITS;
            checkCapacity(addr);
            data[addr] |= getBitMask(i);
            if (maxLength < i) {
                maxLength = i;
            }
        }

        /**
         * Set bit at the given index to 'false'.
         *
         * @param i the index
         */
        public void clear(int i) {
            int addr = i >> ADDRESS_BITS;
            if (addr >= data.length) {
                return;
            }
            data[addr] &= ~getBitMask(i);
        }

        private static long getBitMask(int i) {
            return 1L << (i & ADDRESS_MASK);
        }

        private void checkCapacity(int size) {
            if (size >= data.length) {
                expandCapacity(size);
            }
        }

        private void expandCapacity(int size) {
            while (size >= data.length) {
                int newSize = data.length == 0 ? 1 : data.length * 2;
                long[] d = new long[newSize];
                System.arraycopy(data, 0, d, 0, data.length);
                data = d;
            }
        }

        /**
         * Enable or disable a number of bits.
         *
         * @param fromIndex the index of the first bit to enable or disable
         * @param toIndex one plus the index of the last bit to enable or disable
         * @param value the new value
         */
        public void set(int fromIndex, int toIndex, boolean value) {
            // go backwards so that OutOfMemory happens
            // before some bytes are modified
            for (int i = toIndex - 1; i >= fromIndex; i--) {
                set(i, value);
            }
            if (value) {
                if (toIndex > maxLength) {
                    maxLength = toIndex;
                }
            } else {
                if (toIndex >= maxLength) {
                    maxLength = fromIndex;
                }
            }
        }

        private void set(int i, boolean value) {
            if (value) {
                set(i);
            } else {
                clear(i);
            }
        }

        /**
         * Get the index of the highest set bit plus one, or 0 if no bits are set.
         *
         * @return the length of the bit field
         */
        public int length() {
            int m = maxLength >> ADDRESS_BITS;
            while (m > 0 && data[m] == 0) {
                m--;
            }
            maxLength = (m << ADDRESS_BITS) + (64 - Long.numberOfLeadingZeros(data[m]));
            return maxLength;
        }

    }

}
