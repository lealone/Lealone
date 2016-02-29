/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.lealone.common.util.JdbcUtils;
import org.lealone.common.util.New;
import org.lealone.common.util.StringUtils;
import org.lealone.db.Constants;
import org.lealone.db.util.ScriptReader;

/**
 * Interactive command line tool to access a database using JDBC.
 * 
 * @author H2 Group
 * @author zhh
 */
public class Shell {

    private static final int MAX_ROW_BUFFER = 5000;
    private static final int HISTORY_COUNT = 20;
    // Windows: '\u00b3';
    private static final char BOX_VERTICAL = '|';

    private final PrintStream err = System.err;
    private final InputStream in = System.in;
    private final PrintStream out = System.out;
    private BufferedReader reader;
    private Connection conn;
    private Statement stat;
    private boolean listMode;
    private int maxColumnSize = 100;
    private final ArrayList<String> history = New.arrayList();

    public static void main(String... args) throws SQLException {
        new Shell().run(args);
    }

    private void run(String... args) throws SQLException {
        String url = null;
        String user = "";
        String password = "";
        String sql = null;
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            arg = arg.trim();
            if (arg.isEmpty())
                continue;
            if (arg.equals("-url")) {
                url = args[++i];
            } else if (arg.equals("-user")) {
                user = args[++i];
            } else if (arg.equals("-password")) {
                password = args[++i];
            } else if (arg.equals("-sql")) {
                sql = args[++i];
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else if (arg.equals("-list")) {
                listMode = true;
            } else {
                showUsage();
                return;
            }
        }
        if (url != null) {
            conn = DriverManager.getConnection(url, user, password);
            stat = conn.createStatement();
        }
        if (sql == null) {
            promptLoop();
        } else {
            ScriptReader r = new ScriptReader(new StringReader(sql));
            while (true) {
                String s = r.readStatement();
                if (s == null) {
                    break;
                }
                execute(s);
            }
            if (conn != null) {
                conn.close();
            }
        }
    }

    private void showUsage() {
        println("Options are case sensitive. Supported options are:");
        println("[-help] or [-?]         Print the list of options");
        println("[-url \"<url>\"]          The database URL (jdbc:lealone:...)");
        println("[-user <user>]          The user name");
        println("[-password <pwd>]       The password");
        println("[-sql \"<statements>\"]   Execute the SQL statements and exit");
        println("");
        println("If special characters don't work as expected, ");
        println("you may need to use -Dfile.encoding=UTF-8 (Mac OS X) or CP850 (Windows).");
        println("");
    }

    private void showHelp() {
        println("Commands are case insensitive; SQL statements end with ';'");
        println("help or ?      Display this help");
        println("list           Toggle result list / stack trace mode");
        println("maxwidth       Set maximum column width (default is 100)");
        println("autocommit     Enable or disable autocommit");
        println("history        Show the last 20 statements");
        println("quit or exit   Close the connection and exit");
        println("");
    }

    private void promptLoop() {
        println("");
        println("Welcome to Lealone Shell " + Constants.getVersion());
        println("Exit with Ctrl+C");
        if (conn != null) {
            showHelp();
        }
        String statement = null;
        if (reader == null) {
            reader = new BufferedReader(new InputStreamReader(in));
        }
        while (true) {
            try {
                if (conn == null) {
                    connect();
                    showHelp();
                }
                if (statement == null) {
                    print("sql> ");
                } else {
                    print("...> ");
                }
                String line = readLine();
                if (line == null) {
                    break;
                }
                String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                boolean end = trimmed.endsWith(";");
                if (end) {
                    line = line.substring(0, line.lastIndexOf(';'));
                    trimmed = trimmed.substring(0, trimmed.length() - 1);
                }
                String lower = StringUtils.toLowerEnglish(trimmed);
                if ("exit".equals(lower) || "quit".equals(lower)) {
                    break;
                } else if ("help".equals(lower) || "?".equals(lower)) {
                    showHelp();
                } else if ("list".equals(lower)) {
                    listMode = !listMode;
                    println("Result list mode is now " + (listMode ? "on" : "off"));
                } else if ("history".equals(lower)) {
                    for (int i = 0, size = history.size(); i < size; i++) {
                        String s = history.get(i);
                        s = s.replace('\n', ' ').replace('\r', ' ');
                        println("#" + (1 + i) + ": " + s);
                    }
                    if (history.size() > 0) {
                        println("To re-run a statement, type the number and press and enter");
                    } else {
                        println("No history");
                    }
                } else if (lower.startsWith("autocommit")) {
                    lower = lower.substring("autocommit".length()).trim();
                    if ("true".equals(lower)) {
                        conn.setAutoCommit(true);
                    } else if ("false".equals(lower)) {
                        conn.setAutoCommit(false);
                    } else {
                        println("Usage: autocommit [true|false]");
                    }
                    println("Autocommit is now " + conn.getAutoCommit());
                } else if (lower.startsWith("maxwidth")) {
                    lower = lower.substring("maxwidth".length()).trim();
                    try {
                        maxColumnSize = Integer.parseInt(lower);
                    } catch (NumberFormatException e) {
                        println("Usage: maxwidth <integer value>");
                    }
                    println("Maximum column width is now " + maxColumnSize);
                } else {
                    boolean addToHistory = true;
                    if (statement == null) {
                        if (StringUtils.isNumber(line)) {
                            int pos = Integer.parseInt(line);
                            if (pos == 0 || pos > history.size()) {
                                println("Not found");
                            } else {
                                statement = history.get(pos - 1);
                                addToHistory = false;
                                println(statement);
                                end = true;
                            }
                        } else {
                            statement = line;
                        }
                    } else {
                        statement += "\n" + line;
                    }
                    if (end) {
                        if (addToHistory) {
                            history.add(0, statement);
                            if (history.size() > HISTORY_COUNT) {
                                history.remove(HISTORY_COUNT);
                            }
                        }
                        execute(statement);
                        statement = null;
                    }
                }
            } catch (SQLException e) {
                println("SQL Exception: " + e.getMessage());
                statement = null;
            } catch (IOException e) {
                println(e.getMessage());
                break;
            } catch (Exception e) {
                println("Exception: " + e.toString());
                e.printStackTrace(err);
                break;
            }
        }
        if (conn != null) {
            try {
                conn.close();
                println("Connection closed");
            } catch (SQLException e) {
                println("SQL Exception: " + e.getMessage());
                e.printStackTrace(err);
            }
        }
    }

    private void connect() throws IOException, SQLException {
        StringBuilder buff = new StringBuilder(100);
        buff.append(Constants.URL_PREFIX).append(Constants.URL_TCP).append("//").append("127.0.0.1").append(':')
                .append(Constants.DEFAULT_TCP_PORT).append('/').append("test");
        String url = buff.toString();
        println("[Enter]   " + url);
        print("URL       ");
        url = readLine(url).trim();

        String user = "lealone";
        println("[Enter]   " + user);
        print("User      ");
        user = readLine(user);

        println("[Enter]   Hide");
        String password = readPassword();
        conn = DriverManager.getConnection(url, user, password);
        stat = conn.createStatement();
        println("Connected");
    }

    private void print(String s) {
        out.print(s);
        out.flush();
    }

    private void println(String s) {
        out.println(s);
        out.flush();
    }

    private String readPassword() throws IOException {
        java.io.Console console = System.console();
        if (console != null) {
            char[] password = console.readPassword("Password  ");
            return password == null ? null : new String(password);
        } else { // In Eclipse, use the default solution
            print("Password  ");
            return readLine();
        }
    }

    private String readLine(String defaultValue) throws IOException {
        String s = readLine();
        return s.length() == 0 ? defaultValue : s;
    }

    private String readLine() throws IOException {
        String line = reader.readLine();
        if (line == null) {
            throw new IOException("Aborted");
        }
        return line;
    }

    private void execute(String sql) {
        if (sql.trim().isEmpty()) {
            return;
        }
        long time = System.currentTimeMillis();
        try {
            ResultSet rs = null;
            try {
                if (stat.execute(sql)) {
                    rs = stat.getResultSet();
                    int rowCount = printResult(rs, listMode);
                    time = System.currentTimeMillis() - time;
                    println("(" + rowCount + (rowCount == 1 ? " row, " : " rows, ") + time + " ms)");
                } else {
                    int updateCount = stat.getUpdateCount();
                    time = System.currentTimeMillis() - time;
                    println("(Update count: " + updateCount + ", " + time + " ms)");
                }
            } finally {
                JdbcUtils.closeSilently(rs);
            }
        } catch (SQLException e) {
            println("Error: " + e.toString());
            if (listMode) {
                e.printStackTrace(err);
            }
            return;
        }
        out.println();
    }

    private int printResult(ResultSet rs, boolean asList) throws SQLException {
        if (asList) {
            return printResultAsList(rs);
        }
        return printResultAsTable(rs);
    }

    private int printResultAsTable(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int len = meta.getColumnCount();
        boolean truncated = false;
        ArrayList<String[]> rows = New.arrayList();
        // buffer the header
        String[] columns = new String[len];
        for (int i = 0; i < len; i++) {
            String s = meta.getColumnLabel(i + 1);
            columns[i] = s == null ? "" : s;
        }
        rows.add(columns);
        int rowCount = 0;
        while (rs.next()) {
            rowCount++;
            truncated |= loadRow(rs, len, rows);
            if (rowCount > MAX_ROW_BUFFER) {
                printRows(rows, len);
                rows.clear();
            }
        }
        printRows(rows, len);
        rows.clear();
        if (truncated) {
            println("(data is partially truncated)");
        }
        return rowCount;
    }

    private boolean loadRow(ResultSet rs, int len, ArrayList<String[]> rows) throws SQLException {
        boolean truncated = false;
        String[] row = new String[len];
        for (int i = 0; i < len; i++) {
            String s = rs.getString(i + 1);
            if (s == null) {
                s = "null";
            }
            // only truncate if more than one column
            if (len > 1 && s.length() > maxColumnSize) {
                s = s.substring(0, maxColumnSize);
                truncated = true;
            }
            row[i] = s;
        }
        rows.add(row);
        return truncated;
    }

    private int[] printRows(ArrayList<String[]> rows, int len) {
        int[] columnSizes = new int[len];
        for (int i = 0; i < len; i++) {
            int max = 0;
            for (String[] row : rows) {
                max = Math.max(max, row[i].length());
            }
            if (len > 1) {
                Math.min(maxColumnSize, max);
            }
            columnSizes[i] = max;
        }
        for (String[] row : rows) {
            StringBuilder buff = new StringBuilder();
            for (int i = 0; i < len; i++) {
                if (i > 0) {
                    buff.append(' ').append(BOX_VERTICAL).append(' ');
                }
                String s = row[i];
                buff.append(s);
                if (i < len - 1) {
                    for (int j = s.length(); j < columnSizes[i]; j++) {
                        buff.append(' ');
                    }
                }
            }
            println(buff.toString());
        }
        return columnSizes;
    }

    private int printResultAsList(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int longestLabel = 0;
        int len = meta.getColumnCount();
        String[] columns = new String[len];
        for (int i = 0; i < len; i++) {
            String s = meta.getColumnLabel(i + 1);
            columns[i] = s;
            longestLabel = Math.max(longestLabel, s.length());
        }
        StringBuilder buff = new StringBuilder();
        int rowCount = 0;
        while (rs.next()) {
            rowCount++;
            buff.setLength(0);
            if (rowCount > 1) {
                println("");
            }
            for (int i = 0; i < len; i++) {
                if (i > 0) {
                    buff.append('\n');
                }
                String label = columns[i];
                buff.append(label);
                for (int j = label.length(); j < longestLabel; j++) {
                    buff.append(' ');
                }
                buff.append(": ").append(rs.getString(i + 1));
            }
            println(buff.toString());
        }
        if (rowCount == 0) {
            for (int i = 0; i < len; i++) {
                if (i > 0) {
                    buff.append('\n');
                }
                String label = columns[i];
                buff.append(label);
            }
            println(buff.toString());
        }
        return rowCount;
    }

}
