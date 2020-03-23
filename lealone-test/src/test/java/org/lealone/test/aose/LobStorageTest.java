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
package org.lealone.test.aose;

import java.io.InputStream;
import java.io.StringReader;

import org.junit.Test;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.IOUtils;
import org.lealone.common.util.TempFileDeleter;
import org.lealone.db.Constants;
import org.lealone.db.DataHandler;
import org.lealone.db.value.ValueLob;
import org.lealone.storage.LobStorage;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.lob.LobStreamStorage;
import org.lealone.storage.fs.FileStorage;
import org.lealone.test.TestBase;

public class LobStorageTest extends TestBase {
    @Test
    public void run() throws Exception {
        AOStorage storage = AOStorageTest.openStorage();
        String name = LobStorageTest.class.getName();
        name = "/" + name.replace('.', '/') + ".class";
        InputStream in = LobStorageTest.class.getResourceAsStream(name);
        int length = in.available();
        LobStreamStorage lobStorage = new LobStreamStorage(new DataHandlerMock(), storage);
        lobStorage.init();

        ValueLob lob = lobStorage.createBlob(in, -1);
        assertEquals(1, lob.getLobId());

        in = lobStorage.getInputStream(lob, null, 0); // 返回的流未实现available()
        assertNotNull(in);
        byte[] bytes = IOUtils.toByteArray(in);
        assertEquals(length, bytes.length);

        String clobStr = "clob-test";
        StringBuilder buff = new StringBuilder(1000 * clobStr.length());
        for (int i = 0; i < 1000; i++)
            buff.append(clobStr);

        clobStr = buff.toString();
        StringReader reader = new StringReader(clobStr);
        lob = lobStorage.createClob(reader, -1);
        assertEquals(2, lob.getLobId());

        in = lobStorage.getInputStream(lob, null, 0);
        assertNotNull(in);

        lob = lobStorage.copyLob(lob, 10, lob.getPrecision());
        assertEquals(10, lob.getTableId());

        lobStorage.setTable(lob, 20);
        lobStorage.removeAllForTable(20);
        lobStorage.removeLob(lob);
    }

    private static class DataHandlerMock implements DataHandler {

        @Override
        public String getDatabasePath() {
            return null;
        }

        @Override
        public FileStorage openFile(String name, String mode, boolean mustExist) {
            return null;
        }

        @Override
        public void checkPowerOff() throws DbException {
        }

        @Override
        public void checkWritingAllowed() throws DbException {
        }

        @Override
        public int getMaxLengthInplaceLob() {
            return Constants.DEFAULT_MAX_LENGTH_INPLACE_LOB2;
        }

        @Override
        public String getLobCompressionAlgorithm(int type) {
            return null;
        }

        @Override
        public TempFileDeleter getTempFileDeleter() {
            return null;
        }

        @Override
        public Object getLobSyncObject() {
            return null;
        }

        @Override
        public LobStorage getLobStorage() {
            return null;
        }

        @Override
        public int readLob(long lobId, byte[] hmac, long offset, byte[] buff, int off, int length) {
            return 0;
        }
    }
}
