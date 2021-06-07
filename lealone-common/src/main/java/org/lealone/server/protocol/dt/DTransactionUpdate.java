/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.dt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.statement.StatementUpdate;
import org.lealone.storage.PageKey;

public class DTransactionUpdate extends StatementUpdate {

    public final List<PageKey> pageKeys;

    public DTransactionUpdate(List<PageKey> pageKeys, String sql) {
        super(sql);
        this.pageKeys = pageKeys;
    }

    public DTransactionUpdate(NetInputStream in, int version) throws IOException {
        super(in, version);
        pageKeys = readPageKeys(in);
    }

    @Override
    public PacketType getType() {
        return PacketType.DISTRIBUTED_TRANSACTION_UPDATE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.DISTRIBUTED_TRANSACTION_UPDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        writePageKeys(out, pageKeys);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DTransactionUpdate> {
        @Override
        public DTransactionUpdate decode(NetInputStream in, int version) throws IOException {
            return new DTransactionUpdate(in, version);
        }
    }

    public static void writePageKeys(NetOutputStream out, List<PageKey> pageKeys) throws IOException {
        if (pageKeys == null) {
            out.writeInt(0);
        } else {
            int size = pageKeys.size();
            out.writeInt(size);
            for (int i = 0; i < size; i++) {
                PageKey pk = pageKeys.get(i);
                out.writePageKey(pk);
            }
        }
    }

    public static List<PageKey> readPageKeys(NetInputStream in) throws IOException {
        ArrayList<PageKey> pageKeys;
        int size = in.readInt();
        if (size > 0) {
            pageKeys = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                PageKey pk = in.readPageKey();
                pageKeys.add(pk);
            }
        } else {
            pageKeys = null;
        }
        return pageKeys;
    }
}
