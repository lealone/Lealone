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

import org.lealone.server.protocol.batch.BatchStatementPreparedUpdate;
import org.lealone.server.protocol.batch.BatchStatementUpdate;
import org.lealone.server.protocol.batch.BatchStatementUpdateAck;
import org.lealone.server.protocol.dt.DistributedTransactionAddSavepoint;
import org.lealone.server.protocol.dt.DistributedTransactionCommit;
import org.lealone.server.protocol.dt.DistributedTransactionPreparedQuery;
import org.lealone.server.protocol.dt.DistributedTransactionPreparedQueryAck;
import org.lealone.server.protocol.dt.DistributedTransactionPreparedUpdate;
import org.lealone.server.protocol.dt.DistributedTransactionPreparedUpdateAck;
import org.lealone.server.protocol.dt.DistributedTransactionQuery;
import org.lealone.server.protocol.dt.DistributedTransactionQueryAck;
import org.lealone.server.protocol.dt.DistributedTransactionRollback;
import org.lealone.server.protocol.dt.DistributedTransactionRollbackSavepoint;
import org.lealone.server.protocol.dt.DistributedTransactionUpdate;
import org.lealone.server.protocol.dt.DistributedTransactionUpdateAck;
import org.lealone.server.protocol.dt.DistributedTransactionValidate;
import org.lealone.server.protocol.dt.DistributedTransactionValidateAck;
import org.lealone.server.protocol.lob.LobRead;
import org.lealone.server.protocol.lob.LobReadAck;
import org.lealone.server.protocol.ps.PreparedStatementClose;
import org.lealone.server.protocol.ps.PreparedStatementGetMetaData;
import org.lealone.server.protocol.ps.PreparedStatementGetMetaDataAck;
import org.lealone.server.protocol.ps.PreparedStatementPrepare;
import org.lealone.server.protocol.ps.PreparedStatementPrepareAck;
import org.lealone.server.protocol.ps.PreparedStatementPrepareReadParams;
import org.lealone.server.protocol.ps.PreparedStatementPrepareReadParamsAck;
import org.lealone.server.protocol.ps.PreparedStatementQuery;
import org.lealone.server.protocol.ps.PreparedStatementUpdate;
import org.lealone.server.protocol.replication.ReplicationCheckConflict;
import org.lealone.server.protocol.replication.ReplicationCheckConflictAck;
import org.lealone.server.protocol.replication.ReplicationCommit;
import org.lealone.server.protocol.replication.ReplicationHandleConflict;
import org.lealone.server.protocol.replication.ReplicationPreparedUpdate;
import org.lealone.server.protocol.replication.ReplicationPreparedUpdateAck;
import org.lealone.server.protocol.replication.ReplicationRollback;
import org.lealone.server.protocol.replication.ReplicationUpdate;
import org.lealone.server.protocol.replication.ReplicationUpdateAck;
import org.lealone.server.protocol.result.ResultChangeId;
import org.lealone.server.protocol.result.ResultClose;
import org.lealone.server.protocol.result.ResultFetchRows;
import org.lealone.server.protocol.result.ResultFetchRowsAck;
import org.lealone.server.protocol.result.ResultReset;
import org.lealone.server.protocol.session.SessionCancelStatement;
import org.lealone.server.protocol.session.SessionClose;
import org.lealone.server.protocol.session.SessionInit;
import org.lealone.server.protocol.session.SessionInitAck;
import org.lealone.server.protocol.session.SessionSetAutoCommit;
import org.lealone.server.protocol.statement.StatementQuery;
import org.lealone.server.protocol.statement.StatementQueryAck;
import org.lealone.server.protocol.statement.StatementUpdate;
import org.lealone.server.protocol.statement.StatementUpdateAck;
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

@SuppressWarnings({ "rawtypes", "unchecked" })
public class PacketDecoders {

    private static PacketDecoder[] decoders = new PacketDecoder[PacketType.VOID.value];

    public static void register(PacketType type, PacketDecoder<? extends Packet> decoder) {
        decoders[type.value] = decoder;
    }

    public static PacketDecoder<? extends Packet> getDecoder(PacketType type) {
        return decoders[type.value];
    }

    public static PacketDecoder<? extends Packet> getDecoder(int type) {
        return decoders[type];
    }

    static {
        register(PacketType.SESSION_INIT, SessionInit.decoder);
        register(PacketType.SESSION_INIT_ACK, SessionInitAck.decoder);
        register(PacketType.SESSION_CANCEL_STATEMENT, SessionCancelStatement.decoder);
        register(PacketType.SESSION_SET_AUTO_COMMIT, SessionSetAutoCommit.decoder);
        register(PacketType.SESSION_CLOSE, SessionClose.decoder);

        register(PacketType.PREPARED_STATEMENT_PREPARE, PreparedStatementPrepare.decoder);
        register(PacketType.PREPARED_STATEMENT_PREPARE_ACK, PreparedStatementPrepareAck.decoder);
        register(PacketType.PREPARED_STATEMENT_PREPARE_READ_PARAMS, PreparedStatementPrepareReadParams.decoder);
        register(PacketType.PREPARED_STATEMENT_PREPARE_READ_PARAMS_ACK, PreparedStatementPrepareReadParamsAck.decoder);
        register(PacketType.PREPARED_STATEMENT_QUERY, PreparedStatementQuery.decoder);
        // register(PacketType.PREPARED_STATEMENT_QUERY_ACK, PreparedStatementQueryAck.decoder);
        register(PacketType.PREPARED_STATEMENT_UPDATE, PreparedStatementUpdate.decoder);
        // register(PacketType.PREPARED_STATEMENT_UPDATE_ACK, PreparedStatementUpdateAck.decoder);
        register(PacketType.PREPARED_STATEMENT_GET_META_DATA, PreparedStatementGetMetaData.decoder);
        register(PacketType.PREPARED_STATEMENT_GET_META_DATA_ACK, PreparedStatementGetMetaDataAck.decoder);
        register(PacketType.PREPARED_STATEMENT_CLOSE, PreparedStatementClose.decoder);

        register(PacketType.STATEMENT_QUERY, StatementQuery.decoder);
        register(PacketType.STATEMENT_QUERY_ACK, StatementQueryAck.decoder);
        register(PacketType.STATEMENT_UPDATE, StatementUpdate.decoder);
        register(PacketType.STATEMENT_UPDATE_ACK, StatementUpdateAck.decoder);

        register(PacketType.BATCH_STATEMENT_UPDATE, BatchStatementUpdate.decoder);
        register(PacketType.BATCH_STATEMENT_UPDATE_ACK, BatchStatementUpdateAck.decoder);
        register(PacketType.BATCH_STATEMENT_PREPARED_UPDATE, BatchStatementPreparedUpdate.decoder);

        register(PacketType.RESULT_FETCH_ROWS, ResultFetchRows.decoder);
        register(PacketType.RESULT_FETCH_ROWS_ACK, ResultFetchRowsAck.decoder);
        register(PacketType.RESULT_CHANGE_ID, ResultChangeId.decoder);
        register(PacketType.RESULT_RESET, ResultReset.decoder);
        register(PacketType.RESULT_CLOSE, ResultClose.decoder);

        register(PacketType.LOB_READ, LobRead.decoder);
        register(PacketType.LOB_READ_ACK, LobReadAck.decoder);

        register(PacketType.REPLICATION_UPDATE, ReplicationUpdate.decoder);
        register(PacketType.REPLICATION_UPDATE_ACK, ReplicationUpdateAck.decoder);
        register(PacketType.REPLICATION_PREPARED_UPDATE, ReplicationPreparedUpdate.decoder);
        register(PacketType.REPLICATION_PREPARED_UPDATE_ACK, ReplicationPreparedUpdateAck.decoder);
        register(PacketType.REPLICATION_COMMIT, ReplicationCommit.decoder);
        register(PacketType.REPLICATION_ROLLBACK, ReplicationRollback.decoder);
        register(PacketType.REPLICATION_CHECK_CONFLICT, ReplicationCheckConflict.decoder);
        register(PacketType.REPLICATION_CHECK_CONFLICT_ACK, ReplicationCheckConflictAck.decoder);
        register(PacketType.REPLICATION_HANDLE_CONFLICT, ReplicationHandleConflict.decoder);

        register(PacketType.DISTRIBUTED_TRANSACTION_QUERY, DistributedTransactionQuery.decoder);
        register(PacketType.DISTRIBUTED_TRANSACTION_QUERY_ACK, DistributedTransactionQueryAck.decoder);
        register(PacketType.DISTRIBUTED_TRANSACTION_PREPARED_QUERY, DistributedTransactionPreparedQuery.decoder);
        register(PacketType.DISTRIBUTED_TRANSACTION_PREPARED_QUERY_ACK, DistributedTransactionPreparedQueryAck.decoder);
        register(PacketType.DISTRIBUTED_TRANSACTION_UPDATE, DistributedTransactionUpdate.decoder);
        register(PacketType.DISTRIBUTED_TRANSACTION_UPDATE_ACK, DistributedTransactionUpdateAck.decoder);
        register(PacketType.DISTRIBUTED_TRANSACTION_PREPARED_UPDATE, DistributedTransactionPreparedUpdate.decoder);
        register(PacketType.DISTRIBUTED_TRANSACTION_PREPARED_UPDATE_ACK,
                DistributedTransactionPreparedUpdateAck.decoder);

        register(PacketType.DISTRIBUTED_TRANSACTION_COMMIT, DistributedTransactionCommit.decoder);
        register(PacketType.DISTRIBUTED_TRANSACTION_ROLLBACK, DistributedTransactionRollback.decoder);
        register(PacketType.DISTRIBUTED_TRANSACTION_ADD_SAVEPOINT, DistributedTransactionAddSavepoint.decoder);
        register(PacketType.DISTRIBUTED_TRANSACTION_ROLLBACK_SAVEPOINT,
                DistributedTransactionRollbackSavepoint.decoder);
        register(PacketType.DISTRIBUTED_TRANSACTION_VALIDATE, DistributedTransactionValidate.decoder);
        register(PacketType.DISTRIBUTED_TRANSACTION_VALIDATE_ACK, DistributedTransactionValidateAck.decoder);

        register(PacketType.STORAGE_PUT, StoragePut.decoder);
        register(PacketType.STORAGE_PUT_ACK, StoragePutAck.decoder);
        register(PacketType.STORAGE_APPEND, StorageAppend.decoder);
        register(PacketType.STORAGE_APPEND_ACK, StorageAppendAck.decoder);
        register(PacketType.STORAGE_GET, StorageGet.decoder);
        register(PacketType.STORAGE_GET_ACK, StorageGetAck.decoder);
        register(PacketType.STORAGE_PREPARE_MOVE_LEAF_PAGE, StoragePrepareMoveLeafPage.decoder);
        register(PacketType.STORAGE_PREPARE_MOVE_LEAF_PAGE_ACK, StoragePrepareMoveLeafPageAck.decoder);
        register(PacketType.STORAGE_MOVE_LEAF_PAGE, StorageMoveLeafPage.decoder);
        register(PacketType.STORAGE_REPLICATE_ROOT_PAGES, StorageReplicateRootPages.decoder);
        register(PacketType.STORAGE_READ_PAGE, StorageReadPage.decoder);
        register(PacketType.STORAGE_READ_PAGE_ACK, StorageReadPageAck.decoder);
        register(PacketType.STORAGE_REMOVE_LEAF_PAGE, StorageRemoveLeafPage.decoder);
    }
}
