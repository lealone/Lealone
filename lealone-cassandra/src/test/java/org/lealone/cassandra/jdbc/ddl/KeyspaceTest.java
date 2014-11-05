package org.lealone.cassandra.jdbc.ddl;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import org.lealone.cassandra.jdbc.TestBase;

public class KeyspaceTest extends TestBase {
    @Test
    public void run() throws Exception {
        create();
        alter();
        drop();
    }

    void create() throws Exception {
        sql = "CREATE KEYSPACE IF NOT EXISTS KeyspaceTest " + //
                "WITH replication = {'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true";
        executeUpdate();
    }

    void alter() throws Exception {
        sql = "ALTER KEYSPACE KeyspaceTest " + //
                "WITH replication = {'class':'SimpleStrategy', 'replication_factor':3} AND DURABLE_WRITES = true";
        executeUpdate();
    }

    void drop() throws Exception {
        sql = "DROP KEYSPACE IF EXISTS KeyspaceTest";
        executeUpdate();
        executeUpdate();
        sql = "DROP KEYSPACE KeyspaceTest";
        assertEquals(tryExecuteUpdate(), -1);
    }
}
