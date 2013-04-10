/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.codefollower.lealone.omid.tso;

import java.io.PrintWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Histogram {
    private static final Log LOG = LogFactory.getLog(Histogram.class);
    final private int size;
    final private int[] counts;
    private int max;
    private int min = Integer.MIN_VALUE;

    public Histogram(int size) {
        this.size = size;
        this.counts = new int[size];
    }

    public void add(int i) {
        if (i >= size) {
            LOG.error("Tried to add " + i + " which is bigger than size " + size);
            return;
        }
        counts[i]++;
        if (i > max) {
            max = i;
        }
        if (i < min) {
            min = i;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(max).append('\n');
        for (int i = 0; i <= max; ++i) {
            sb.append("[").append(i).append("]\t");
        }
        sb.append('\n');
        for (int i = 0; i <= max; ++i) {
            sb.append(counts[i]).append("\t");
        }
        return sb.toString();
    }

    public void log() {
        for (int i = min; i <= max; ++i) {
            LOG.debug(String.format("[%5d]\t%5d", i, counts[i]));
        }
    }

    public void print(PrintWriter writer) {
        for (int i = 0; i <= max; ++i) {
            writer.format("%5d\t%5d\n", i, counts[i]);
        }
    }
}
