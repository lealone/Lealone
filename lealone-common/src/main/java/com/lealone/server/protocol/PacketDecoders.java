/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol;

import com.lealone.server.protocol.batch.BatchStatementPreparedUpdate;
import com.lealone.server.protocol.batch.BatchStatementUpdate;
import com.lealone.server.protocol.batch.BatchStatementUpdateAck;
import com.lealone.server.protocol.lob.LobRead;
import com.lealone.server.protocol.lob.LobReadAck;
import com.lealone.server.protocol.ps.PreparedStatementClose;
import com.lealone.server.protocol.ps.PreparedStatementGetMetaData;
import com.lealone.server.protocol.ps.PreparedStatementGetMetaDataAck;
import com.lealone.server.protocol.ps.PreparedStatementPrepare;
import com.lealone.server.protocol.ps.PreparedStatementPrepareAck;
import com.lealone.server.protocol.ps.PreparedStatementPrepareReadParams;
import com.lealone.server.protocol.ps.PreparedStatementPrepareReadParamsAck;
import com.lealone.server.protocol.ps.PreparedStatementQuery;
import com.lealone.server.protocol.ps.PreparedStatementUpdate;
import com.lealone.server.protocol.result.ResultChangeId;
import com.lealone.server.protocol.result.ResultClose;
import com.lealone.server.protocol.result.ResultFetchRows;
import com.lealone.server.protocol.result.ResultFetchRowsAck;
import com.lealone.server.protocol.result.ResultReset;
import com.lealone.server.protocol.session.SessionCancelStatement;
import com.lealone.server.protocol.session.SessionClose;
import com.lealone.server.protocol.session.SessionInit;
import com.lealone.server.protocol.session.SessionInitAck;
import com.lealone.server.protocol.session.SessionSetAutoCommit;
import com.lealone.server.protocol.session.SessionTransactionStatement;
import com.lealone.server.protocol.statement.StatementQuery;
import com.lealone.server.protocol.statement.StatementQueryAck;
import com.lealone.server.protocol.statement.StatementUpdate;
import com.lealone.server.protocol.statement.StatementUpdateAck;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class PacketDecoders {

    private static PacketDecoder[] decoders = new PacketDecoder[PacketType.VOID.value];

    public static void register(PacketType type, PacketDecoder<? extends Packet> decoder) {
        decoders[type.value] = decoder;
    }

    public static PacketDecoder<? extends Packet> getDecoder(PacketType type) {
        return getDecoder(type.value);
    }

    public static PacketDecoder<? extends Packet> getDecoder(int type) {
        if (type < 0 || type >= decoders.length)
            return null;
        else
            return decoders[type];
    }

    static {
        register(PacketType.SESSION_INIT, SessionInit.decoder);
        register(PacketType.SESSION_INIT_ACK, SessionInitAck.decoder);
        register(PacketType.SESSION_CANCEL_STATEMENT, SessionCancelStatement.decoder);
        register(PacketType.SESSION_SET_AUTO_COMMIT, SessionSetAutoCommit.decoder);
        register(PacketType.SESSION_CLOSE, SessionClose.decoder);
        register(PacketType.SESSION_TRANSACTION_STATEMENT, SessionTransactionStatement.decoder);

        register(PacketType.PREPARED_STATEMENT_PREPARE, PreparedStatementPrepare.decoder);
        register(PacketType.PREPARED_STATEMENT_PREPARE_ACK, PreparedStatementPrepareAck.decoder);
        register(PacketType.PREPARED_STATEMENT_PREPARE_READ_PARAMS,
                PreparedStatementPrepareReadParams.decoder);
        register(PacketType.PREPARED_STATEMENT_PREPARE_READ_PARAMS_ACK,
                PreparedStatementPrepareReadParamsAck.decoder);
        register(PacketType.PREPARED_STATEMENT_QUERY, PreparedStatementQuery.decoder);
        // register(PacketType.PREPARED_STATEMENT_QUERY_ACK, PreparedStatementQueryAck.decoder);
        register(PacketType.PREPARED_STATEMENT_UPDATE, PreparedStatementUpdate.decoder);
        // register(PacketType.PREPARED_STATEMENT_UPDATE_ACK, PreparedStatementUpdateAck.decoder);
        register(PacketType.PREPARED_STATEMENT_GET_META_DATA, PreparedStatementGetMetaData.decoder);
        register(PacketType.PREPARED_STATEMENT_GET_META_DATA_ACK,
                PreparedStatementGetMetaDataAck.decoder);
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
    }
}
