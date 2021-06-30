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
import org.lealone.storage.PageKey;

public class DTransactionParameters {

    public final List<PageKey> pageKeys;
    public final String indexName;
    public final boolean autoCommit;

    public DTransactionParameters(List<PageKey> pageKeys, String indexName, boolean autoCommit) {
        this.pageKeys = pageKeys;
        this.indexName = indexName;
        this.autoCommit = autoCommit;
    }

    public DTransactionParameters(NetInputStream in, int version) throws IOException {
        pageKeys = readPageKeys(in);
        indexName = in.readString();
        autoCommit = in.readBoolean();
    }

    public void encode(NetOutputStream out, int version) throws IOException {
        writePageKeys(out, pageKeys);
        out.writeString(indexName);
        out.writeBoolean(autoCommit);
    }

    private static void writePageKeys(NetOutputStream out, List<PageKey> pageKeys) throws IOException {
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

    private static List<PageKey> readPageKeys(NetInputStream in) throws IOException {
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
