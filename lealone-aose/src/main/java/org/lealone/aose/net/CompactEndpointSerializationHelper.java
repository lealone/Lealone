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
package org.lealone.aose.net;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;

import org.lealone.net.NetEndpoint;

public class CompactEndpointSerializationHelper {
    public static void serialize(NetEndpoint endpoint, DataOutput out) throws IOException {
        byte[] buf = endpoint.getAddress();
        out.writeByte(buf.length);
        out.write(buf);
        out.writeInt(endpoint.getPort());
    }

    public static NetEndpoint deserialize(DataInput in) throws IOException {
        byte[] bytes = new byte[in.readByte()];
        in.readFully(bytes, 0, bytes.length);
        int port = in.readInt();
        return new NetEndpoint(InetAddress.getByAddress(bytes), port);
    }

    public static int serializedSize(NetEndpoint from) {
        if (from.geInetAddress() instanceof Inet4Address)
            return 1 + 4 + 4;
        assert from.geInetAddress() instanceof Inet6Address;
        return 1 + 16 + 4;
    }
}
