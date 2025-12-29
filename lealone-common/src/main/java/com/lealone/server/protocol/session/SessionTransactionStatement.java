/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol.session;

import java.io.IOException;

import com.lealone.net.NetInputStream;
import com.lealone.net.NetOutputStream;
import com.lealone.server.protocol.Packet;
import com.lealone.server.protocol.PacketDecoder;
import com.lealone.server.protocol.PacketType;

public class SessionTransactionStatement implements Packet {

    /**
     * The type of a COMMIT statement.
     */
    public static final int COMMIT = 123;

    /**
     * The type of a ROLLBACK statement.
     */
    public static final int ROLLBACK = 124;

    /**
     * The type of a SAVEPOINT statement.
     */
    public static final int SAVEPOINT = 127;

    /**
     * The type of a ROLLBACK TO SAVEPOINT statement.
     */
    public static final int ROLLBACK_TO_SAVEPOINT = 128;

    private final int statementType;
    private String savepointName;

    public SessionTransactionStatement(int statementType) {
        this.statementType = statementType;
    }

    @Override
    public PacketType getType() {
        return PacketType.SESSION_TRANSACTION_STATEMENT;
    }

    @Override
    public PacketType getAckType() {
        // 保持兼容，老版本是通过执行TransactionStatement实现
        return PacketType.STATEMENT_UPDATE_ACK;
    }

    public int getStatementType() {
        return statementType;
    }

    public String getSavepointName() {
        return savepointName;
    }

    public void setSavepointName(String name) {
        this.savepointName = name;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(statementType);
        if (isSavepoint(statementType))
            out.writeString(savepointName);
    }

    private static boolean isSavepoint(int statementType) {
        return statementType == SAVEPOINT || statementType == ROLLBACK_TO_SAVEPOINT;

    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<SessionTransactionStatement> {
        @Override
        public SessionTransactionStatement decode(NetInputStream in, int version) throws IOException {
            int statementType = in.readInt();
            SessionTransactionStatement p = new SessionTransactionStatement(statementType);
            if (isSavepoint(statementType))
                p.setSavepointName(in.readString());
            return p;
        }
    }
}
