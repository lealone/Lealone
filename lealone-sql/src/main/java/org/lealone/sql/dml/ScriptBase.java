/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.lealone.common.compress.CompressTool;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.security.SHA256;
import org.lealone.common.util.IOUtils;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.SysProperties;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.expression.Expression;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.fs.FileStorageInputStream;
import org.lealone.storage.fs.FileStorageOutputStream;
import org.lealone.storage.fs.FileUtils;

/**
 * This class is the base for RunScript and GenScript.
 */
abstract class ScriptBase extends ManipulationStatement {

    /**
     * The default name of the script file if .zip compression is used.
     */
    private static final String SCRIPT_SQL = "script.sql";

    /**
     * The output stream.
     */
    protected OutputStream out;

    /**
     * The input stream.
     */
    protected InputStream in;

    /**
     * The file name (if set).
     */
    private Expression fileNameExpr;

    private Expression password;

    private String fileName;

    private String cipher;
    private FileStorage fileStorage;
    private String compressionAlgorithm;

    ScriptBase(ServerSession session) {
        super(session);
    }

    public void setCipher(String c) {
        cipher = c;
    }

    private boolean isEncrypted() {
        return cipher != null;
    }

    public void setPassword(Expression password) {
        this.password = password;
    }

    public void setFileNameExpr(Expression file) {
        this.fileNameExpr = file;
    }

    protected String getFileName() {
        if (fileNameExpr != null && fileName == null) {
            fileName = fileNameExpr.optimize(session).getValue(session).getString();
            if (fileName == null || fileName.trim().length() == 0) {
                fileName = "script.sql";
            }
            // 如果是一个不包含目录的文件名，自动加上ScriptDirectory
            if (FileUtils.getName(fileName).equals(fileName)) {
                String dir = SysProperties.getScriptDirectory();
                fileName = FileUtils.getDirWithSeparator(dir) + fileName;
            }
        }
        return fileName;
    }

    /**
     * Delete the target file.
     */
    void deleteStore() {
        String file = getFileName();
        if (file != null) {
            FileUtils.delete(file);
        }
    }

    private void initStore() {
        Database db = session.getDatabase();
        byte[] key = null;
        if (cipher != null && password != null) {
            char[] pass = password.optimize(session).getValue(session).getString().toCharArray();
            key = SHA256.getKeyPasswordHash("script", pass);
        }
        String file = getFileName();
        fileStorage = FileStorage.open(db, file, "rw", cipher, key);
        fileStorage.setCheckedWriting(false);
        fileStorage.init();
    }

    /**
     * Open the output stream.
     */
    void openOutput() {
        String file = getFileName();
        if (file == null) {
            return;
        }
        if (isEncrypted()) {
            initStore();
            out = new FileStorageOutputStream(fileStorage, session.getDatabase(), compressionAlgorithm);
            // always use a big buffer, otherwise end-of-block is written a lot
            out = new BufferedOutputStream(out, Constants.IO_BUFFER_SIZE_COMPRESS);
        } else {
            OutputStream o;
            try {
                o = FileUtils.newOutputStream(file, false);
            } catch (IOException e) {
                throw DbException.convertIOException(e, null);
            }
            out = new BufferedOutputStream(o, Constants.IO_BUFFER_SIZE);
            out = CompressTool.wrapOutputStream(out, compressionAlgorithm, SCRIPT_SQL);
        }
    }

    /**
     * Open the input stream.
     */
    void openInput() {
        String file = getFileName();
        if (file == null) {
            return;
        }
        if (isEncrypted()) {
            initStore();
            in = new FileStorageInputStream(fileStorage, session.getDatabase(),
                    compressionAlgorithm != null, false);
        } else {
            InputStream inStream;
            try {
                inStream = FileUtils.newInputStream(file);
            } catch (IOException e) {
                throw DbException.convertIOException(e, file);
            }
            in = new BufferedInputStream(inStream, Constants.IO_BUFFER_SIZE);
            in = CompressTool.wrapInputStream(in, compressionAlgorithm, SCRIPT_SQL);
            if (in == null) {
                throw DbException.get(ErrorCode.FILE_NOT_FOUND_1, SCRIPT_SQL + " in " + file);
            }
        }
    }

    /**
     * Close input and output streams.
     */
    void closeIO() {
        IOUtils.closeSilently(out);
        out = null;
        IOUtils.closeSilently(in);
        in = null;
        if (fileStorage != null) {
            fileStorage.closeSilently();
            fileStorage = null;
        }
    }

    @Override
    public boolean needRecompile() {
        return false;
    }

    public void setCompressionAlgorithm(String algorithm) {
        this.compressionAlgorithm = algorithm;
    }
}
