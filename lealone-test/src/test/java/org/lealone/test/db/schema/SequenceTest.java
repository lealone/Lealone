/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.db.schema;

import org.junit.Test;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.result.Result;
import org.lealone.db.schema.Sequence;
import org.lealone.test.db.DbObjectTestBase;

public class SequenceTest extends DbObjectTestBase {

    @Test
    public void run() {
        create();
        alter();
        drop();
    }

    void create() {
        executeUpdate("DROP TABLE IF EXISTS CreateSequenceTest");
        executeUpdate("DROP SEQUENCE IF EXISTS myseq");

        executeUpdate("CREATE SEQUENCE IF NOT EXISTS myseq" //
                + " START WITH 1000 INCREMENT BY 1 MINVALUE 10 MAXVALUE 10000 CYCLE CACHE 3 BELONGS_TO_TABLE");

        assertNotNull(schema.findSequence(session, "myseq"));

        executeUpdate("CREATE TABLE IF NOT EXISTS CreateSequenceTest(id int primary key, f1 int)");
        executeUpdate("INSERT INTO CreateSequenceTest(id, f1) VALUES(1, myseq.NEXTVAL)"); // 1000
        executeUpdate("INSERT INTO CreateSequenceTest(id, f1) VALUES(2, myseq.NEXTVAL)"); // 1001

        sql = "SELECT f1 FROM CreateSequenceTest where id = 2";
        assertEquals(1001, getInt(sql, 1));
        executeUpdate("DROP TABLE IF EXISTS CreateSequenceTest");

        sql = "select myseq.CURRVAL, myseq.NEXTVAL";
        Result rs = executeQuery(sql);
        assertTrue(rs.next());
        assertEquals(1001, getInt(rs, 1));
        assertEquals(1002, getInt(rs, 2));

        rs = executeQuery(sql);
        assertTrue(rs.next());
        assertEquals(1002, getInt(rs, 1));
        assertEquals(1003, getInt(rs, 2));
    }

    void alter() {
        Sequence sequence = schema.findSequence(session, "myseq");
        assertEquals(10000, sequence.getMaxValue());
        executeUpdate("ALTER SEQUENCE myseq MAXVALUE 20000");
        // sequence变动了需要重新取
        sequence = schema.findSequence(session, "myseq");
        assertEquals(20000, sequence.getMaxValue());

        executeUpdate("drop table if exists t1_myseq2");
        executeUpdate("drop sequence if exists myseq2");
        executeUpdate("create sequence if not exists myseq2");
        executeUpdate("create table t1_myseq2(f1 int default (next value for myseq2), f2 int)");

        executeUpdate("insert into t1_myseq2(f2) values(2)");
        Result rs = executeQuery("select f1 from t1_myseq2 where f2=2");
        assertTrue(rs.next());
        assertEquals(1, getInt(rs, 1));
        rs.close();

        executeUpdate("alter sequence myseq2 restart with 50");
        executeUpdate("insert into t1_myseq2(f2) values(3)");
        rs = executeQuery("select f1 from t1_myseq2 where f2=3");
        assertTrue(rs.next());
        assertEquals(50, getInt(rs, 1));
        rs.close();
    }

    void drop() {
        try {
            executeUpdate("DROP SEQUENCE IF EXISTS myseq");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.SEQUENCE_BELONGS_TO_A_TABLE_1);
        }

        schema.findSequence(session, "myseq").setBelongsToTable(false);
        executeUpdate("DROP SEQUENCE IF EXISTS myseq");
        assertNull(schema.findSequence(session, "myseq"));
    }
}
