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
package org.lealone.transaction.amte.log;

import java.util.LinkedList;

import org.lealone.db.DataBuffer;
import org.lealone.transaction.amte.AMTransactionEngine;
import org.lealone.transaction.amte.TransactionalLogRecord;

public class LazyLog {

    final AMTransactionEngine transactionEngine;
    final long transactionId;
    final LinkedList<TransactionalLogRecord> logRecords;

    public LazyLog(AMTransactionEngine transactionEngine, long transactionId,
            LinkedList<TransactionalLogRecord> logRecords) {
        this.transactionEngine = transactionEngine;
        this.transactionId = transactionId;
        this.logRecords = logRecords;
    }

    public RedoLogRecord createRedoLogRecord(DataBuffer writeBuffer) {
        if (logRecords.isEmpty())
            return null;
        RedoLogRecord redoLogRecord = RedoLogRecord.createLocalTransactionRedoLogRecord(transactionId, null);
        redoLogRecord.writeHead(writeBuffer);
        int pos = writeBuffer.position();
        writeBuffer.putInt(0);
        for (TransactionalLogRecord r : logRecords) {
            r.writeForRedo(writeBuffer, transactionEngine);
        }
        int length = writeBuffer.position() - pos - 4;
        writeBuffer.putInt(pos, length);
        return redoLogRecord;
    }
}
