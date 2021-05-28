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
