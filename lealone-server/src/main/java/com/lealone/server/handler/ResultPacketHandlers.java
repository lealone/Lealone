/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.handler;

import com.lealone.db.ManualCloseable;
import com.lealone.db.result.Result;
import com.lealone.db.session.ServerSession;
import com.lealone.server.protocol.Packet;
import com.lealone.server.protocol.PacketType;
import com.lealone.server.protocol.result.ResultChangeId;
import com.lealone.server.protocol.result.ResultClose;
import com.lealone.server.protocol.result.ResultFetchRows;
import com.lealone.server.protocol.result.ResultFetchRowsAck;
import com.lealone.server.protocol.result.ResultReset;

class ResultPacketHandlers extends PacketHandlers {

    static void register() {
        register(PacketType.RESULT_FETCH_ROWS, new FetchRows());
        register(PacketType.RESULT_CHANGE_ID, new ChangeId());
        register(PacketType.RESULT_RESET, new Reset());
        register(PacketType.RESULT_CLOSE, new Close());
    }

    private static class FetchRows implements PacketHandler<ResultFetchRows> {
        @Override
        public Packet handle(ServerSession session, ResultFetchRows packet) {
            Result result = (Result) session.getCache(packet.resultId);
            return new ResultFetchRowsAck(result, packet.count);
        }
    }

    private static class ChangeId implements PacketHandler<ResultChangeId> {
        @Override
        public Packet handle(ServerSession session, ResultChangeId packet) {
            ManualCloseable obj = session.removeCache(packet.oldId, false);
            session.addCache(packet.newId, obj);
            return null;
        }
    }

    private static class Reset implements PacketHandler<ResultReset> {
        @Override
        public Packet handle(ServerSession session, ResultReset packet) {
            Result result = (Result) session.getCache(packet.resultId);
            result.reset();
            return null;
        }
    }

    private static class Close implements PacketHandler<ResultClose> {
        @Override
        public Packet handle(ServerSession session, ResultClose packet) {
            Result result = (Result) session.removeCache(packet.resultId, true);
            if (result != null) {
                result.close();
            }
            return null;
        }
    }
}
