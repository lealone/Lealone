/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.yourbase.store;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;

import com.codefollower.yourbase.constant.Constants;
import com.codefollower.yourbase.constant.ErrorCode;
import com.codefollower.yourbase.engine.ConnectionInfo;
import com.codefollower.yourbase.engine.Database;
import com.codefollower.yourbase.engine.RegularDatabaseEngine;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.message.DbException;
import com.codefollower.yourbase.store.fs.FilePathRec;
import com.codefollower.yourbase.store.fs.FileUtils;
import com.codefollower.yourbase.store.fs.Recorder;
import com.codefollower.yourbase.tools.Recover;
import com.codefollower.yourbase.util.IOUtils;
import com.codefollower.yourbase.util.New;
import com.codefollower.yourbase.util.StringUtils;
import com.codefollower.yourbase.util.Utils;

/**
 * A tool that simulates a crash while writing to the database, and then
 * verifies the database doesn't get corrupt.
 */
public class RecoverTester implements Recorder {

    private static RecoverTester instance;

    private String testDatabase = "memFS:reopen";
    private int writeCount = Utils.getProperty("yourbase.recoverTestOffset", 0);
    private int testEvery = Utils.getProperty("yourbase.recoverTest", 64);
    private final long maxFileSize = Utils.getProperty("yourbase.recoverTestMaxFileSize", Integer.MAX_VALUE) * 1024L * 1024;
    private int verifyCount;
    private final HashSet<String> knownErrors = New.hashSet();
    private volatile boolean testing;

    /**
     * Initialize the recover test.
     *
     * @param recoverTest the value of the recover test parameter
     */
    public static synchronized void init(String recoverTest) {
        RecoverTester tester = RecoverTester.getInstance();
        if (StringUtils.isNumber(recoverTest)) {
            tester.setTestEvery(Integer.parseInt(recoverTest));
        }
        FilePathRec.setRecorder(tester);
    }

    public static synchronized RecoverTester getInstance() {
        if (instance == null) {
            instance = new RecoverTester();
        }
        return instance;
    }

    public void log(int op, String fileName, byte[] data, long x) {
        if (op != Recorder.WRITE && op != Recorder.TRUNCATE) {
            return;
        }
        if (!fileName.endsWith(Constants.SUFFIX_PAGE_FILE)) {
            return;
        }
        writeCount++;
        if ((writeCount % testEvery) != 0) {
            return;
        }
        if (FileUtils.size(fileName) > maxFileSize) {
            // System.out.println(fileName + " " + IOUtils.length(fileName));
            return;
        }
        if (testing) {
            // avoid deadlocks
            return;
        }
        testing = true;
        PrintWriter out = null;
        try {
            out = new PrintWriter(
                    new OutputStreamWriter(
                    FileUtils.newOutputStream(fileName + ".log", true)));
            testDatabase(fileName, out);
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        } finally {
            IOUtils.closeSilently(out);
            testing = false;
        }
    }

    private synchronized void testDatabase(String fileName, PrintWriter out) {
        out.println("+ write #" + writeCount + " verify #" + verifyCount);
        try {
            IOUtils.copyFiles(fileName, testDatabase + Constants.SUFFIX_PAGE_FILE);
            verifyCount++;
            // avoid using the Engine class to avoid deadlocks
            Properties p = new Properties();
            p.setProperty("user", "");
            p.setProperty("password", "");
            ConnectionInfo ci = new ConnectionInfo("jdbc:yourbase:" + testDatabase + ";FILE_LOCK=NO;TRACE_LEVEL_FILE=0", p);
            Database database = RegularDatabaseEngine.getInstance().createDatabase();
            database.init(ci, null);
            // close the database
            Session session = database.getSystemSession();
            session.prepare("script to '" + testDatabase + ".sql'").query(0);
            session.prepare("shutdown immediately").update();
            database.removeSession(null);
            // everything OK - return
            return;
        } catch (DbException e) {
            SQLException e2 = DbException.toSQLException(e);
            int errorCode = e2.getErrorCode();
            if (errorCode == ErrorCode.WRONG_USER_OR_PASSWORD) {
                return;
            } else if (errorCode == ErrorCode.FILE_ENCRYPTION_ERROR_1) {
                return;
            }
            e.printStackTrace(System.out);
        } catch (Exception e) {
            // failed
            int errorCode = 0;
            if (e instanceof SQLException) {
                errorCode = ((SQLException) e).getErrorCode();
            }
            if (errorCode == ErrorCode.WRONG_USER_OR_PASSWORD) {
                return;
            } else if (errorCode == ErrorCode.FILE_ENCRYPTION_ERROR_1) {
                return;
            }
            e.printStackTrace(System.out);
        }
        out.println("begin ------------------------------ " + writeCount);
        try {
            Recover.execute(fileName.substring(0, fileName.lastIndexOf('/')), null);
        } catch (SQLException e) {
            // ignore
        }
        testDatabase += "X";
        try {
            IOUtils.copyFiles(fileName, testDatabase + Constants.SUFFIX_PAGE_FILE);
            // avoid using the Engine class to avoid deadlocks
            Properties p = new Properties();
            ConnectionInfo ci = new ConnectionInfo("jdbc:yourbase:" + testDatabase + ";FILE_LOCK=NO", p);
            Database database = RegularDatabaseEngine.getInstance().createDatabase();
            database.init(ci, null);
            // close the database
            database.removeSession(null);
        } catch (Exception e) {
            int errorCode = 0;
            if (e instanceof DbException) {
                e = ((DbException) e).getSQLException();
                errorCode = ((SQLException) e).getErrorCode();
            }
            if (errorCode == ErrorCode.WRONG_USER_OR_PASSWORD) {
                return;
            } else if (errorCode == ErrorCode.FILE_ENCRYPTION_ERROR_1) {
                return;
            }
            StringBuilder buff = new StringBuilder();
            StackTraceElement[] list = e.getStackTrace();
            for (int i = 0; i < 10 && i < list.length; i++) {
                buff.append(list[i].toString()).append('\n');
            }
            String s = buff.toString();
            if (!knownErrors.contains(s)) {
                out.println(writeCount + " code: " + errorCode + " " + e.toString());
                e.printStackTrace(System.out);
                knownErrors.add(s);
            } else {
                out.println(writeCount + " code: " + errorCode);
            }
        }
    }

    public void setTestEvery(int testEvery) {
        this.testEvery = testEvery;
    }

}