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
package org.lealone.common.logging;

import org.lealone.common.logging.spi.LogDelegate;

public class ConsoleLogDelegate implements LogDelegate {

    ConsoleLogDelegate() {
    }

    private void log(Object message) {
        System.out.println(message);
    }

    private void log(Object message, Object... params) {
        char[] chars = message.toString().toCharArray();
        int length = chars.length;
        StringBuilder s = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            if (chars[i] == '{' && chars[i + 1] == '}') {
                s.append("%s");
                i++;
            } else {
                s.append(chars[i]);
            }
        }
        System.out.println(String.format(s.toString(), params));
    }

    private void log(Object message, Throwable t) {
        log(message);
        if (t != null)
            t.printStackTrace(System.err);
    }

    private void log(Object message, Throwable t, Object... params) {
        log(message, params);
        if (t != null)
            t.printStackTrace(System.err);
    }

    @Override
    public boolean isWarnEnabled() {
        return true;
    }

    @Override
    public boolean isInfoEnabled() {
        return true;
    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public boolean isTraceEnabled() {
        return false;
    }

    @Override
    public void fatal(Object message) {
        log(message);
    }

    @Override
    public void fatal(Object message, Throwable t) {
        log(message, t);
    }

    @Override
    public void error(Object message) {
        log(message);
    }

    @Override
    public void error(Object message, Object... params) {
        log(message, params);
    }

    @Override
    public void error(Object message, Throwable t) {
        log(message, t);
    }

    @Override
    public void error(Object message, Throwable t, Object... params) {
        log(message, t, params);
    }

    @Override
    public void warn(Object message) {
        log(message);
    }

    @Override
    public void warn(Object message, Object... params) {
        log(message, params);
    }

    @Override
    public void warn(Object message, Throwable t) {
        log(message, t);
    }

    @Override
    public void warn(Object message, Throwable t, Object... params) {
        log(message, t, params);
    }

    @Override
    public void info(Object message) {
        log(message);
    }

    @Override
    public void info(Object message, Object... params) {
        log(message, params);
    }

    @Override
    public void info(Object message, Throwable t) {
        log(message, t);
    }

    @Override
    public void info(Object message, Throwable t, Object... params) {
        log(message, t, params);
    }

    @Override
    public void debug(Object message) {
        log(message);
    }

    @Override
    public void debug(Object message, Object... params) {
        log(message, params);
    }

    @Override
    public void debug(Object message, Throwable t) {
        log(message, t);
    }

    @Override
    public void debug(Object message, Throwable t, Object... params) {
        log(message, t, params);
    }

    @Override
    public void trace(Object message) {
        log(message);
    }

    @Override
    public void trace(Object message, Object... params) {
        log(message, params);
    }

    @Override
    public void trace(Object message, Throwable t) {
        log(message, t);
    }

    @Override
    public void trace(Object message, Throwable t, Object... params) {
        log(message, t, params);
    }

}
