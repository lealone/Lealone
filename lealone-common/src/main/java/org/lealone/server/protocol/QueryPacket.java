/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;

public abstract class QueryPacket implements Packet {

    public final int resultId;
    public final int maxRows;
    public final int fetchSize;
    public final boolean scrollable;

    public QueryPacket(int resultId, int maxRows, int fetchSize, boolean scrollable) {
        this.resultId = resultId;
        this.maxRows = maxRows;
        this.fetchSize = fetchSize;
        this.scrollable = scrollable;
    }

    public QueryPacket(NetInputStream in, int version) throws IOException {
        resultId = in.readInt();
        maxRows = in.readInt();
        fetchSize = in.readInt();
        scrollable = in.readBoolean();
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(resultId).writeInt(maxRows).writeInt(fetchSize).writeBoolean(scrollable);
    }
}
