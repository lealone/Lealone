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
package com.codefollower.lealone.atomicdb.cql.functions;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

import com.codefollower.lealone.atomicdb.db.marshal.LongType;
import com.codefollower.lealone.atomicdb.db.marshal.TimeUUIDType;
import com.codefollower.lealone.atomicdb.db.marshal.TimestampType;
import com.codefollower.lealone.atomicdb.utils.ByteBufferUtil;
import com.codefollower.lealone.atomicdb.utils.UUIDGen;

public abstract class TimeuuidFcts
{
    public static final Function nowFct = new AbstractFunction("now", TimeUUIDType.instance)
    {
        public ByteBuffer execute(List<ByteBuffer> parameters)
        {
            return ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes());
        }

        @Override
        public boolean isPure()
        {
            return false;
        }
    };

    public static final Function minTimeuuidFct = new AbstractFunction("mintimeuuid", TimeUUIDType.instance, TimestampType.instance)
    {
        public ByteBuffer execute(List<ByteBuffer> parameters)
        {
            ByteBuffer bb = parameters.get(0);
            if (bb == null)
                return null;

            return ByteBuffer.wrap(UUIDGen.decompose(UUIDGen.minTimeUUID(TimestampType.instance.compose(bb).getTime())));
        }
    };

    public static final Function maxTimeuuidFct = new AbstractFunction("maxtimeuuid", TimeUUIDType.instance, TimestampType.instance)
    {
        public ByteBuffer execute(List<ByteBuffer> parameters)
        {
            ByteBuffer bb = parameters.get(0);
            if (bb == null)
                return null;

            return ByteBuffer.wrap(UUIDGen.decompose(UUIDGen.maxTimeUUID(TimestampType.instance.compose(bb).getTime())));
        }
    };

    public static final Function dateOfFct = new AbstractFunction("dateof", TimestampType.instance, TimeUUIDType.instance)
    {
        public ByteBuffer execute(List<ByteBuffer> parameters)
        {
            ByteBuffer bb = parameters.get(0);
            if (bb == null)
                return null;

            return TimestampType.instance.decompose(new Date(UUIDGen.unixTimestamp(UUIDGen.getUUID(bb))));
        }
    };

    public static final Function unixTimestampOfFct = new AbstractFunction("unixtimestampof", LongType.instance, TimeUUIDType.instance)
    {
        public ByteBuffer execute(List<ByteBuffer> parameters)
        {
            ByteBuffer bb = parameters.get(0);
            if (bb == null)
                return null;

            return ByteBufferUtil.bytes(UUIDGen.unixTimestamp(UUIDGen.getUUID(bb)));
        }
    };
}

