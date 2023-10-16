/*
 * Copyright 1999-2012 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.mysql.server.protocol;

import org.lealone.common.exceptions.DbException;

/**
 * @author xianmao.hexm
 * @author zhh
 */
public abstract class Packet {

    public int packetLength;
    public byte packetId;

    /**
     * 取得数据包信息
     */
    public abstract String getPacketInfo();

    public void read(PacketInput in) {
        throw DbException.throwInternalError("read");
    }

    public void write(PacketOutput out) {
        throw DbException.throwInternalError("write");
    }

    @Override
    public String toString() {
        return new StringBuilder().append(getPacketInfo()) //
                .append("{length=").append(packetLength) //
                .append(",id=").append(packetId).append('}').toString();
    }
}
