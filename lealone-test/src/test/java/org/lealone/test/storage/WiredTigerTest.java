/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.lealone.test.storage;

import java.io.File;

import org.junit.Test;
import org.lealone.test.sql.TestBase;

import com.wiredtiger.db.Connection;
import com.wiredtiger.db.Cursor;
import com.wiredtiger.db.Session;
import com.wiredtiger.db.wiredtiger;

public class WiredTigerTest {
    public static Connection getWTConnection() {
        String dir = TestBase.test_dir + "/WiredTigerTest";
        if (!new File(dir).exists())
            new File(dir).mkdirs();
        return wiredtiger.open(dir, "create");
    }

    @Test
    public void testCursor() {
        Connection wtConnection = getWTConnection();
        Session wtSession = wtConnection.open_session(null);
        wtSession.create("table:lealone_map_id", "key_format=S,value_format=i");
        Cursor wtCursor = wtSession.open_cursor("table:lealone_map_id", null, "append");

        try {
            String name = "test";
            int id;
            name += "_map_id";
            wtCursor.putKeyString(name);
            if (wtCursor.search() == 0) {
                id = wtCursor.getValueInt();
            } else {
                wtCursor.putKeyString("max_id");
                if (wtCursor.search() == 0) {
                    id = wtCursor.getValueInt();
                    wtCursor.putKeyString("max_id");
                    wtCursor.putValueInt(id + 1);
                    wtCursor.update();
                } else {
                    id = 1;
                    wtCursor.putKeyString("max_id");
                    wtCursor.putValueInt(id + 1);
                    wtCursor.insert();
                }
                wtCursor.putKeyString(name);
                wtCursor.putValueInt(id);
                wtCursor.insert();
            }
            wtCursor.close();

            wtCursor = wtSession.open_cursor("table:lealone_map_id", null, "append");
            while (wtCursor.next() == 0) {
                System.out.println(wtCursor.getKeyString() + "  " + wtCursor.getValueInt());
            }

            wtCursor.reset();
            while (wtCursor.prev() == 0) {
                System.out.println(wtCursor.getKeyString() + "  " + wtCursor.getValueInt());
            }

        } finally {
            wtCursor.close();
            wtSession.close(null);
            wtConnection.close(null);
        }
    }
}
