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
package org.lealone.common.trace;

class NoTrace implements Trace {

    NoTrace() {
    }

    @Override
    public int getTraceId() {
        return -1;
    }

    @Override
    public String getTraceObjectName() {
        return "";
    }

    @Override
    public NoTrace setType(TraceModuleType type) {
        return this;
    }

    @Override
    public boolean isInfoEnabled() {
        return false;
    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public void error(Throwable t, String s) {
    }

    @Override
    public void error(Throwable t, String s, Object... params) {
    }

    @Override
    public void info(String s) {
    }

    @Override
    public void info(String s, Object... params) {
    }

    @Override
    public void info(Throwable t, String s) {
    }

    @Override
    public void infoSQL(String sql, String params, int count, long time) {
    }

    @Override
    public void infoCode(String format, Object... args) {
    }

    @Override
    public void debug(String s, Object... params) {
    }

    @Override
    public void debug(String s) {
    }

    @Override
    public void debug(Throwable t, String s) {
    }

    @Override
    public void debugCode(String java) {
    }
}
