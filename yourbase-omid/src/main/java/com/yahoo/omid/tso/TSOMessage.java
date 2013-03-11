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

import java.io.DataOutputStream;
import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

/**
 * The interface the identified the messages sent to TSO
 * @author maysam
 *
 */
public interface TSOMessage {

    final public byte CommittedTransactionReportByteByte = (byte) 0xf0;
    final public byte CommittedTransactionReportShortByte = (byte) 0xf1;
    final public byte CommittedTransactionReportIntegerByte = (byte) 0xf2;
    final public byte CommittedTransactionReportLongByte = (byte) 0xf3;
    final public byte CommittedTransactionReportByteShort = (byte) 0xf4;
    final public byte CommittedTransactionReportShortShort = (byte) 0xf5;
    final public byte CommittedTransactionReportIntegerShort = (byte) 0xf6;
    final public byte CommittedTransactionReportLongShort = (byte) 0xf7;

    final public byte CommittedTransactionReport = (byte) 0xe0;

    final public byte TimestampRequest = (byte) 0xc0;
    final public byte TimestampResponse = (byte) 0xc1;
    final public byte CommitRequest = (byte) 0xc2;
    final public byte CommitResponse = (byte) 0xc3;
    final public byte FullAbortReport = (byte) 0xc4;
    final public byte CommitQueryRequest = (byte) 0xc5;
    final public byte CommitQueryResponse = (byte) 0xc6;
    final public byte AbortedTransactionReport = (byte) 0xc7;
    final public byte CleanedTransactionReport = (byte) 0xc8;
    final public byte LargestDeletedTimestampReport = (byte) 0xc9;
    final public byte CleanedTransactionReportByte = (byte) 0xca;
    final public byte AbortedTransactionReportByte = (byte) 0xcb;
    final public byte AbortRequest = (byte) 0xcc;
    final public byte ZipperState = (byte) 0xcd;

    /*
     * Deserialize function
     * We use ChannelBuffer instead of DataInputStream because the performance is better
     */
    public void readObject(ChannelBuffer aInputStream);

    /*
     * Serialize function
     */
    public void writeObject(DataOutputStream aOutputStream) throws IOException;

}
