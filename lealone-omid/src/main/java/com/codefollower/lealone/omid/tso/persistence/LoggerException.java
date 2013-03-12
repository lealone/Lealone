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

package com.codefollower.lealone.omid.tso.persistence;

@SuppressWarnings("serial")
public abstract class LoggerException extends Exception {

    private int code;

    LoggerException(int code) {
        this.code = code;
    }

    public interface Code {
        int OK = 0;
        int ADDFAILED = -1;
        int INITLOCKFAILED = -2;
        int BKOPFAILED = -3;
        int ZKOPFAILED = -4;
        int LOGGERDISABLED = -5;

        int ILLEGALOP = -101;
    }

    /**
     * Create an exception from an error code
     * @param code return error code
     * @return corresponding exception
     */
    public static LoggerException create(int code) {
        switch (code) {
        case Code.ADDFAILED:
            return new AddFailedException();
        case Code.INITLOCKFAILED:
            return new InitLockFailedException();
        case Code.BKOPFAILED:
            return new BKOpFailedException();
        case Code.ZKOPFAILED:
            return new ZKOpFailedException();
        case Code.LOGGERDISABLED:
            return new LoggerDisabledException();
        default:
            return new IllegalOpException();
        }

    }

    public void setCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return this.code;
    }

    public static String getMessage(int code) {
        switch (code) {
        case Code.OK:
            return "No problem";
        case Code.ADDFAILED:
            return "Error while reading ledger";
        case Code.INITLOCKFAILED:
            return "Failed to obtain zookeeper lock";
        case Code.BKOPFAILED:
            return "BookKeeper operation failed";
        case Code.ZKOPFAILED:
            return "ZooKeeper operation failed";
        case Code.LOGGERDISABLED:
            return "Logger disabled";
        default:
            return "Invalid operation";
        }
    }

    public static class AddFailedException extends LoggerException {
        public AddFailedException() {
            super(Code.ADDFAILED);
        }
    }

    public static class InitLockFailedException extends LoggerException {
        public InitLockFailedException() {
            super(Code.INITLOCKFAILED);
        }
    }

    public static class LoggerDisabledException extends LoggerException {
        public LoggerDisabledException() {
            super(Code.LOGGERDISABLED);
        }
    }

    public static class BKOpFailedException extends LoggerException {
        public BKOpFailedException() {
            super(Code.BKOPFAILED);
        }
    }

    public static class ZKOpFailedException extends LoggerException {
        public ZKOpFailedException() {
            super(Code.ZKOPFAILED);
        }
    }

    public static class IllegalOpException extends LoggerException {
        public IllegalOpException() {
            super(Code.ILLEGALOP);
        }
    }

}
