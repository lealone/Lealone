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

package com.yahoo.omid.tso;

/**
 * The wrapper for timestamp value
 * @author maysam
 *
 */
public class Timestamp {

    public static int counter = 0;

    /**
     * the actual timestamp
     */
    private long timestamp;

    /**
     * getter and setter
     */
    public long get() {
        return timestamp;
    }

    public void set(long t) {
        timestamp = t;
    }

    public void set(Timestamp tr) {
        timestamp = tr.get();
    }

    /**
     * monotonically increase the timestamp value
     */
    public void inc() {
        timestamp++;
    }

    /**
     * compare two timestamps
     * return true if this.timestamp > t
     */
    public boolean greaterThan(Timestamp t) {
        return timestamp > t.get();
    }

    /**
     * Constructor 
     * @param t
     */
    public Timestamp(long t) {
        this.set(t);
        counter++;
    }

    public void finalize() {
        counter--;
    }

    /**
     * Constructor 
     * @param tr
     */
    public Timestamp(Timestamp tr) {
        this.set(tr);
        counter++;
    }

    @Override
    public String toString() {
        return "Timestamp: " + timestamp;
    }
}
