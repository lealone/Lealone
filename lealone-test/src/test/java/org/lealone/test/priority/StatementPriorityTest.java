package org.lealone.test.priority;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.lealone.test.sql.SqlTestBase;

public class StatementPriorityTest extends SqlTestBase {

    public static void main(String[] args) throws Exception {
        new StatementPriorityTest().run();
    }

    static Connection getConn(String url) throws Exception {
        return DriverManager.getConnection(url);
    }

    class MyThread extends Thread {
        Connection connection;
        Statement statement;

        boolean insert;

        public MyThread(boolean insert) {
            this.insert = insert;
            try {
                connection = StatementPriorityTest.this.getConnection();
                statement = connection.createStatement();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Random random = new Random();

        @Override
        public void run() {
            try {
                if (insert) {
                    insert();
                } else {
                    select();
                }
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }

        public void close() throws SQLException {
            statement.close();
            connection.close();
        }

        void insert() throws Exception {
            int loop = 10002;
            for (int i = 10000; i < loop; i++) {
                String sql = "INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('" + i + "', 'a1', 'b', 51)";
                statement.executeUpdate(sql);
            }
        }

        void select() throws Exception {
            int loop = 10;
            while (loop-- > 0) {
                try {
                    stmt.executeQuery("SELECT * FROM StatementPriorityTest");
                    // stmt.executeQuery("SELECT * FROM StatementPriorityTest where pk = " + random.nextInt(20000));
                    // statement.executeQuery(StatementPriorityTest.this.sql);
                    // Thread.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public void run() throws Exception {
        init();
        sql = "select * from StatementPriorityTest where pk = '01'";
        // oneThread();
        multiThreads();
        tearDownAfterClass();
    }

    public void multiThreads() throws Exception {
        int count = 2;
        MyThread[] insertThreads = new MyThread[count];

        MyThread[] selectThreads = new MyThread[count];

        for (int i = 0; i < count; i++) {
            insertThreads[i] = new MyThread(true);
            selectThreads[i] = new MyThread(false);
        }

        for (int i = 0; i < count; i++) {
            insertThreads[i].start();
            selectThreads[i].start();
        }

        for (int i = 0; i < count; i++) {
            insertThreads[i].join();
            selectThreads[i].join();
        }

        for (int i = 0; i < count; i++) {
            insertThreads[i].close();
            selectThreads[i].close();
        }
    }

    public void oneThread() throws Exception {
        int count = 342;
        Connection[] connections = new Connection[count];

        Statement[] statements = new Statement[count];

        for (int i = 0; i < count; i++) {
            connections[i] = getConnection();
            statements[i] = connections[i].createStatement();
        }

        int loop = 1000000;
        while (loop-- > 0) {
            for (int i = 0; i < count; i++) {
                statements[i].executeQuery(sql);
            }
            // Thread.sleep(1000);

            Thread.sleep(500);
        }

        for (int i = 0; i < count; i++) {
            statements[i].close();
            connections[i].close();
        }
    }

    void init() throws Exception {
        createTable("StatementPriorityTest");
        executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('01', 'a1', 'b', 51)");
        executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('02', 'a1', 'b', 61)");
        executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('03', 'a1', 'b', 61)");

        executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('25', 'a2', 'b', 51)");
        executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('26', 'a2', 'b', 61)");
        executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('27', 'a2', 'b', 61)");

        executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('50', 'a1', 'b', 12)");
        executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('51', 'a2', 'b', 12)");
        executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('52', 'a1', 'b', 12)");

        executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('75', 'a1', 'b', 12)");
        executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('76', 'a2', 'b', 12)");
        executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('77', 'a1', 'b', 12)");

        for (int i = 1000; i < 2000; i++)
            executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('" + i + "', 'a1', 'b', 51)");
    }
}
