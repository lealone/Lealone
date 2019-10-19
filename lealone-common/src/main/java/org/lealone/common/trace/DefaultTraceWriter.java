/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.common.trace;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.exceptions.JdbcSQLException;
import org.lealone.common.util.IOUtils;
import org.lealone.db.Constants;
import org.lealone.db.api.ErrorCode;
import org.lealone.storage.fs.FileUtils;

/**
 * @author H2 Group
 * @author zhh
 */
public class DefaultTraceWriter implements TraceWriter {

    /**
     * The default maximum trace file size. It is currently 64 MB. Additionally,
     * there could be a .old file of the same size.
     */
    private static final int DEFAULT_MAX_FILE_SIZE = 64 * 1024 * 1024;

    private static final int CHECK_SIZE_EACH_WRITES = 4096;

    private int levelSystemOut = TraceSystem.DEFAULT_TRACE_LEVEL_SYSTEM_OUT;
    private int levelFile = TraceSystem.DEFAULT_TRACE_LEVEL_FILE;
    private int levelMax;
    private int maxFileSize = DEFAULT_MAX_FILE_SIZE;
    private String fileName;
    private SimpleDateFormat dateFormat;
    private Writer fileWriter;
    private PrintWriter printWriter;
    private int checkSize;
    private boolean closed;
    private boolean writingErrorLogged;
    private TraceWriter writer = this;
    private final PrintStream sysOut = System.out;

    public DefaultTraceWriter(String fileName) {
        this.fileName = fileName;
        updateLevel();
    }

    TraceWriter getTraceWriter() {
        return writer;
    }

    private void updateLevel() {
        levelMax = Math.max(levelSystemOut, levelFile);
    }

    void setFileName(String name) {
        this.fileName = name;
    }

    void setMaxFileSize(int max) {
        this.maxFileSize = max;
    }

    void setLevelSystemOut(int level) {
        levelSystemOut = level;
        updateLevel();
    }

    void setLevelFile(int level) {
        if (level == TraceSystem.ADAPTER) {
            writer = new TraceWriterAdapter();
            String name = fileName;
            if (name != null) {
                if (name.endsWith(Constants.SUFFIX_TRACE_FILE)) {
                    name = name.substring(0, name.length() - Constants.SUFFIX_TRACE_FILE.length());
                }
                int idx = Math.max(name.lastIndexOf('/'), name.lastIndexOf('\\'));
                if (idx >= 0) {
                    name = name.substring(idx + 1);
                }
                writer.setName(name);
            }
        }
        levelFile = level;
        updateLevel();
    }

    /**
     * Close the writers, and the files if required. It is still possible to
     * write after closing, however after each write the file is closed again
     * (slowing down tracing).
     */
    void close() {
        closeWriter();
        closed = true;
    }

    @Override
    public void setName(String name) {
        // nothing to do (the file name is already set)
    }

    @Override
    public boolean isEnabled(int level) {
        return level <= this.levelMax;
    }

    @Override
    public void write(int level, String module, String s, Throwable t) {
        if (level <= levelSystemOut || level > this.levelMax) {
            // level <= levelSystemOut: the system out level is set higher
            // level > this.level: the level for this module is set higher
            sysOut.println(format(module, s));
            if (t != null && levelSystemOut == TraceSystem.DEBUG) {
                t.printStackTrace(sysOut);
            }
        }
        if (fileName != null) {
            if (level <= levelFile) {
                writeFile(format(module, s), t);
            }
        }
    }

    private synchronized String format(String module, String s) {
        if (dateFormat == null) {
            dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ");
        }
        return dateFormat.format(System.currentTimeMillis()) + module + ": " + s;
    }

    private synchronized void writeFile(String s, Throwable t) {
        try {
            if (checkSize++ >= CHECK_SIZE_EACH_WRITES) {
                checkSize = 0;
                closeWriter();
                if (maxFileSize > 0 && FileUtils.size(fileName) > maxFileSize) {
                    String old = fileName + ".old";
                    FileUtils.delete(old);
                    FileUtils.move(fileName, old);
                }
            }
            if (!openWriter()) {
                return;
            }
            printWriter.println(s);
            if (t != null) {
                if (levelFile == TraceSystem.ERROR && t instanceof JdbcSQLException) {
                    JdbcSQLException se = (JdbcSQLException) t;
                    int code = se.getErrorCode();
                    if (ErrorCode.isCommon(code)) {
                        printWriter.println(t.toString());
                    } else {
                        t.printStackTrace(printWriter);
                    }
                } else {
                    t.printStackTrace(printWriter);
                }
            }
            printWriter.flush();
            if (closed) {
                closeWriter();
            }
        } catch (Exception e) {
            logWritingError(e);
        }
    }

    private void logWritingError(Exception e) {
        if (writingErrorLogged) {
            return;
        }
        writingErrorLogged = true;
        Exception se = DbException.get(ErrorCode.TRACE_FILE_ERROR_2, e, fileName, e.toString());
        // print this error only once
        fileName = null;
        sysOut.println(se);
        se.printStackTrace();
    }

    private boolean openWriter() {
        if (printWriter == null) {
            try {
                FileUtils.createDirectories(FileUtils.getParent(fileName));
                if (FileUtils.exists(fileName) && !FileUtils.canWrite(fileName)) {
                    // read only database: don't log error if the trace file
                    // can't be opened
                    return false;
                }
                fileWriter = IOUtils.getBufferedWriter(FileUtils.newOutputStream(fileName, true));
                printWriter = new PrintWriter(fileWriter, true);
            } catch (Exception e) {
                logWritingError(e);
                return false;
            }
        }
        return true;
    }

    private synchronized void closeWriter() {
        if (printWriter != null) {
            printWriter.flush();
            printWriter.close();
            printWriter = null;
        }
        if (fileWriter != null) {
            try {
                fileWriter.close();
            } catch (IOException e) {
                // ignore
            }
            fileWriter = null;
        }
    }
}
