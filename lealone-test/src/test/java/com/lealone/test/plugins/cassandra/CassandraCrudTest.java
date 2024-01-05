/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.plugins.cassandra;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

// 需要运行cassandra，用 cassandra -f 运行
public class CassandraCrudTest extends CassandraTestBase {

    public static void main(String[] args) {
        new CassandraCrudTest().start();
    }

    private AtomicInteger id = new AtomicInteger();

    private void start() {
        int port = TEST_PORT;
        // port = CASSANDRA_PORT;
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", port))
                .withAuthCredentials("cassandra", "cassandra").build()) {
            ResultSet rs = session.execute("select release_version from system.local");
            Row row = rs.one();
            System.out.println(row.getString("release_version"));

            createKeyspace(session);
            createTable(session);
            try {
                crud(session);
            } catch (Exception e) {
                e.printStackTrace();
            }

            // rs = session.execute("select f2 from btest.test where f1<10 ALLOW FILTERING");
            // List<Row> rows = rs.all();
            // System.out.println("row count: " + rows.size());
            //
            // rs = session.execute("select count(*) as cnt from btest.test");
            // row = rs.one();
            // System.out.println("row count: " + row.getLong("cnt"));
        }
    }

    private void createKeyspace(CqlSession session) {
        String sql = "CREATE KEYSPACE IF NOT EXISTS btest "
                + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
        session.execute(sql);
    }

    private void createTable(CqlSession session) {
        session.execute("use btest");
        session.execute("DROP TABLE IF EXISTS test");
        session.execute("CREATE TABLE test (f1 int PRIMARY KEY, f2 int)");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void crud(CqlSession session) throws Exception {
        String sql = "insert into test(f1,f2) values(" + id.incrementAndGet() + ",1)";
        session.execute(sql);

        CountDownLatch latch = new CountDownLatch(1);
        session.executeAsync(sql).handle(new BiFunction() {
            @Override
            public Object apply(Object t, Object u) {
                latch.countDown();
                return null;
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        PreparedStatement statement = session.prepare("insert into test(f1,f2) values(?,1)");
        BatchStatement batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
        for (int i = 0; i < 10; i++) {
            BoundStatement boundStmt = statement.bind();
            boundStmt.setInt(0, id.incrementAndGet());
            batchStatement.add(boundStmt);
        }
        session.execute(batchStatement);
    }
}
