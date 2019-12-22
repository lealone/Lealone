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
package org.lealone.server.protocol;

import java.util.HashMap;

import org.lealone.server.protocol.batch.BatchStatementPreparedUpdate;
import org.lealone.server.protocol.batch.BatchStatementUpdate;
import org.lealone.server.protocol.batch.BatchStatementUpdateAck;
import org.lealone.server.protocol.dt.DistributedTransactionAddSavepoint;
import org.lealone.server.protocol.dt.DistributedTransactionCommit;
import org.lealone.server.protocol.dt.DistributedTransactionRollback;
import org.lealone.server.protocol.dt.DistributedTransactionRollbackSavepoint;
import org.lealone.server.protocol.dt.DistributedTransactionValidate;
import org.lealone.server.protocol.dt.DistributedTransactionValidateAck;
import org.lealone.server.protocol.replication.ReplicationCheckConflict;
import org.lealone.server.protocol.replication.ReplicationCheckConflictAck;
import org.lealone.server.protocol.replication.ReplicationCommit;
import org.lealone.server.protocol.replication.ReplicationHandleConflict;
import org.lealone.server.protocol.replication.ReplicationRollback;
import org.lealone.server.protocol.result.ResultChangeId;
import org.lealone.server.protocol.result.ResultClose;
import org.lealone.server.protocol.result.ResultFetchRows;
import org.lealone.server.protocol.result.ResultFetchRowsAck;
import org.lealone.server.protocol.result.ResultReset;
import org.lealone.server.protocol.storage.StorageAppend;
import org.lealone.server.protocol.storage.StorageAppendAck;
import org.lealone.server.protocol.storage.StorageGet;
import org.lealone.server.protocol.storage.StorageGetAck;
import org.lealone.server.protocol.storage.StorageMoveLeafPage;
import org.lealone.server.protocol.storage.StoragePrepareMoveLeafPage;
import org.lealone.server.protocol.storage.StoragePrepareMoveLeafPageAck;
import org.lealone.server.protocol.storage.StoragePut;
import org.lealone.server.protocol.storage.StoragePutAck;
import org.lealone.server.protocol.storage.StorageReadPage;
import org.lealone.server.protocol.storage.StorageReadPageAck;
import org.lealone.server.protocol.storage.StorageRemoveLeafPage;
import org.lealone.server.protocol.storage.StorageReplicateRootPages;

public class PacketDecoders {

    private static HashMap<Integer, PacketDecoder<? extends Packet>> decoders = new HashMap<>();

    public static void register(PacketType type, PacketDecoder<? extends Packet> decoder) {
        decoders.put(type.value, decoder);
    }

    public static PacketDecoder<? extends Packet> getDecoder(PacketType type) {
        return decoders.get(type.value);
    }

    public static PacketDecoder<? extends Packet> getDecoder(int type) {
        return decoders.get(type);
    }

    static {
        register(PacketType.RESULT_FETCH_ROWS, ResultFetchRows.decoder);
        register(PacketType.RESULT_FETCH_ROWS_ACK, ResultFetchRowsAck.decoder);
        register(PacketType.RESULT_CHANGE_ID, ResultChangeId.decoder);
        register(PacketType.RESULT_RESET, ResultReset.decoder);
        register(PacketType.RESULT_CLOSE, ResultClose.decoder);

        register(PacketType.COMMAND_BATCH_STATEMENT_UPDATE, BatchStatementUpdate.decoder);
        register(PacketType.COMMAND_BATCH_STATEMENT_UPDATE_ACK, BatchStatementUpdateAck.decoder);
        register(PacketType.COMMAND_BATCH_STATEMENT_PREPARED_UPDATE, BatchStatementPreparedUpdate.decoder);

        register(PacketType.COMMAND_READ_LOB, ReadLob.decoder);
        register(PacketType.COMMAND_READ_LOB_ACK, ReadLobAck.decoder);

        register(PacketType.COMMAND_REPLICATION_COMMIT, ReplicationCommit.decoder);
        register(PacketType.COMMAND_REPLICATION_ROLLBACK, ReplicationRollback.decoder);
        register(PacketType.COMMAND_REPLICATION_CHECK_CONFLICT, ReplicationCheckConflict.decoder);
        register(PacketType.COMMAND_REPLICATION_CHECK_CONFLICT_ACK, ReplicationCheckConflictAck.decoder);
        register(PacketType.COMMAND_REPLICATION_HANDLE_CONFLICT, ReplicationHandleConflict.decoder);

        register(PacketType.COMMAND_DISTRIBUTED_TRANSACTION_COMMIT, DistributedTransactionCommit.decoder);
        register(PacketType.COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK, DistributedTransactionRollback.decoder);
        register(PacketType.COMMAND_DISTRIBUTED_TRANSACTION_ADD_SAVEPOINT, DistributedTransactionAddSavepoint.decoder);
        register(PacketType.COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK_SAVEPOINT,
                DistributedTransactionRollbackSavepoint.decoder);
        register(PacketType.COMMAND_DISTRIBUTED_TRANSACTION_VALIDATE, DistributedTransactionValidate.decoder);
        register(PacketType.COMMAND_DISTRIBUTED_TRANSACTION_VALIDATE_ACK, DistributedTransactionValidateAck.decoder);

        register(PacketType.COMMAND_STORAGE_PUT, StoragePut.decoder);
        register(PacketType.COMMAND_STORAGE_PUT_ACK, StoragePutAck.decoder);
        register(PacketType.COMMAND_STORAGE_APPEND, StorageAppend.decoder);
        register(PacketType.COMMAND_STORAGE_APPEND_ACK, StorageAppendAck.decoder);
        register(PacketType.COMMAND_STORAGE_GET, StorageGet.decoder);
        register(PacketType.COMMAND_STORAGE_GET_ACK, StorageGetAck.decoder);
        register(PacketType.COMMAND_STORAGE_PREPARE_MOVE_LEAF_PAGE, StoragePrepareMoveLeafPage.decoder);
        register(PacketType.COMMAND_STORAGE_PREPARE_MOVE_LEAF_PAGE_ACK, StoragePrepareMoveLeafPageAck.decoder);
        register(PacketType.COMMAND_STORAGE_MOVE_LEAF_PAGE, StorageMoveLeafPage.decoder);
        register(PacketType.COMMAND_STORAGE_REPLICATE_ROOT_PAGES, StorageReplicateRootPages.decoder);
        register(PacketType.COMMAND_STORAGE_READ_PAGE, StorageReadPage.decoder);
        register(PacketType.COMMAND_STORAGE_READ_PAGE_ACK, StorageReadPageAck.decoder);
        register(PacketType.COMMAND_STORAGE_REMOVE_LEAF_PAGE, StorageRemoveLeafPage.decoder);
    }
}
