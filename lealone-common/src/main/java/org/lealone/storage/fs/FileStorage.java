/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.storage.fs;

import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.Reference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Map;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.TempFileDeleter;
import org.lealone.db.Constants;
import org.lealone.db.DataHandler;
import org.lealone.db.SysProperties;
import org.lealone.storage.fs.impl.disk.FilePathDisk;
import org.lealone.storage.fs.impl.encrypt.FileEncrypt;
import org.lealone.storage.fs.impl.encrypt.FilePathEncrypt;
import org.lealone.storage.fs.impl.nio.FilePathNio;

/**
 * This class is an abstraction of a random access file.
 * Each file contains a magic header, and reading / writing is done in blocks.
 * 
 * @author H2 Group
 * @author zhh
 */
public class FileStorage {

    /**
     * Open a non encrypted file store with the given settings.
     *
     * @param handler the data handler
     * @param fileName the file name
     * @param mode the access mode (r, rw, rws, rwd)
     * @return the created object
     */
    public static FileStorage open(DataHandler handler, String fileName, String mode) {
        return open(handler, fileName, mode, null, null);
    }

    /**
     * Open an encrypted file store with the given settings.
     *
     * @param handler the data handler
     * @param fileName the file name
     * @param mode the access mode (r, rw, rws, rwd)
     * @param cipher the name of the cipher algorithm
     * @param key the encryption key
     * @return the created object
     */
    public static FileStorage open(DataHandler handler, String fileName, //
            String mode, String cipher, byte[] key) {
        CaseInsensitiveMap<Object> config = new CaseInsensitiveMap<>();
        if (mode != null)
            config.put("mode", mode);
        if (cipher != null)
            config.put("cipher", cipher);
        if (key != null)
            config.put("encryptionKey", key);
        FileStorage fs = open(fileName, config);
        if (handler != null) {
            fs.handler = handler;
            fs.tempFileDeleter = handler.getTempFileDeleter();
        }
        return fs;
    }

    public static FileStorage open(String fileName, Map<String, ?> config) {
        return new FileStorage(fileName, config);
    }

    /**
     * The file name.
     */
    private String fileName;

    /**
     * The callback object is responsible to check access rights, and free up
     * disk space if required.
     */
    private DataHandler handler;

    private long filePos;
    private Reference<?> autoDeleteReference;
    private boolean checkedWriting = true;
    private String mode;
    private TempFileDeleter tempFileDeleter;

    /**
     * The number of read operations.
     */
    private long readCount;

    /**
     * The number of read bytes.
     */
    private long readBytes;

    /**
     * The number of write operations.
     */
    private long writeCount;

    /**
     * The number of written bytes.
     */
    private long writeBytes;

    /**
     * Whether this storage is read-only.
     */
    private boolean readOnly;

    /**
     * The file size (cached).
     */
    private long fileSize;

    /**
     * The file.
     */
    private FileChannel file;

    /**
     * The encrypted file (if encryption is used).
     */
    private FileChannel encryptedFile;

    /**
     * The file lock.
     */
    private FileLock fileLock;

    protected FileStorage(String fileName, Map<String, ?> config) {
        this.fileName = fileName;
        Object encryptionKey = config.get("encryptionKey");
        boolean readOnly = config.containsKey("readOnly");
        mode = (String) config.get("mode");
        if (fileName != null) {
            FilePath p = FilePath.get(fileName);
            // if no explicit scheme was specified, NIO is used
            if (p instanceof FilePathDisk && !fileName.startsWith(p.getScheme() + ":")) {
                // ensure the NIO file system is registered
                FilePathNio.class.getName();
                fileName = "nio:" + fileName;
            }
        }
        FilePath f = FilePath.get(fileName);
        FilePath parent = f.getParent();
        if (parent != null && !parent.exists()) {
            throw DataUtils.newIllegalArgumentException("Directory does not exist: {0}", parent);
        }
        if (f.exists() && !f.canWrite()) {
            readOnly = true;
            mode = "r";
        }
        this.readOnly = readOnly;
        try {
            file = f.open(readOnly ? "r" : "rw");
            if (encryptionKey != null) {
                byte[] key;
                if (encryptionKey instanceof char[])
                    key = FilePathEncrypt.getPasswordBytes((char[]) encryptionKey);
                else
                    key = (byte[]) encryptionKey;
                encryptedFile = file;
                String cipher = (String) config.get("cipher");
                file = new FileEncrypt(fileName, cipher, key, file);
            }
            try {
                if (readOnly) {
                    fileLock = file.tryLock(0, Long.MAX_VALUE, true);
                } else {
                    fileLock = file.tryLock();
                }
            } catch (OverlappingFileLockException e) {
                throw newISE(DataUtils.ERROR_FILE_LOCKED, "The file is locked: {0}", fileName, e);
            }
            if (fileLock == null) {
                throw newISE(DataUtils.ERROR_FILE_LOCKED, "The file is locked: {0}", fileName);
            }
            fileSize = file.size();
        } catch (IOException e) {
            throw newISE(DataUtils.ERROR_READING_FAILED, "Could not open file {0}", fileName, e);
        }
    }

    public static IllegalStateException newISE(int errorCode, String message, Object... arguments) {
        return DataUtils.newIllegalStateException(errorCode, message, arguments);
    }

    public void setCheckedWriting(boolean value) {
        this.checkedWriting = value;
    }

    private void checkWritingAllowed() {
        if (handler != null && checkedWriting) {
            handler.checkWritingAllowed();
        }
    }

    private void checkPowerOff() {
        if (handler != null) {
            handler.checkPowerOff();
        }
    }

    /**
     * Read a number of bytes.
     *
     * @param b the target buffer
     * @param off the offset
     * @param len the number of bytes to read
     */
    public void readFully(byte[] b, int off, int len) {
        // if (SysProperties.CHECK && (len < 0 || len % Constants.FILE_BLOCK_SIZE != 0)) {
        // DbException.throwInternalError("unaligned read " + fileName + " len " + len);
        // }
        checkPowerOff();
        try {
            FileUtils.readFully(file, ByteBuffer.wrap(b, off, len));
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
        filePos += len;
    }

    /**
     * Go to the specified file location.
     *
     * @param pos the location
     */
    public void seek(long pos) {
        if (SysProperties.CHECK && pos % Constants.FILE_BLOCK_SIZE != 0) {
            DbException.throwInternalError("unaligned seek " + fileName + " pos " + pos);
        }
        try {
            if (pos != filePos) {
                file.position(pos);
                filePos = pos;
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
    }

    /**
     * Write a number of bytes.
     *
     * @param b the source buffer
     * @param off the offset
     * @param len the number of bytes to write
     */
    public void write(byte[] b, int off, int len) {
        if (SysProperties.CHECK && (len < 0 || len % Constants.FILE_BLOCK_SIZE != 0)) {
            DbException.throwInternalError("unaligned write " + fileName + " len " + len);
        }
        checkWritingAllowed();
        checkPowerOff();
        try {
            FileUtils.writeFully(file, ByteBuffer.wrap(b, off, len));
        } catch (IOException e) {
            closeFileSilently();
            throw DbException.convertIOException(e, fileName);
        }
        filePos += len;
        fileSize = Math.max(filePos, fileSize);
    }

    /**
     * Set the length of the file. This will expand or shrink the file.
     *
     * @param newLength the new file size
     */
    public void setLength(long newLength) {
        if (SysProperties.CHECK && newLength % Constants.FILE_BLOCK_SIZE != 0) {
            DbException.throwInternalError("unaligned setLength " + fileName + " pos " + newLength);
        }
        checkPowerOff();
        checkWritingAllowed();
        try {
            if (newLength > fileSize) {
                long pos = filePos;
                file.position(newLength - 1);
                FileUtils.writeFully(file, ByteBuffer.wrap(new byte[1]));
                file.position(pos);
            } else {
                file.truncate(newLength);
            }
            fileSize = newLength;
        } catch (IOException e) {
            closeFileSilently();
            throw DbException.convertIOException(e, fileName);
        }
    }

    /**
     * Get the file size in bytes.
     *
     * @return the file size
     */
    public long length() {
        try {
            long len = fileSize;
            if (SysProperties.CHECK2) {
                len = file.size();
                if (len != fileSize) {
                    DbException.throwInternalError("file " + fileName //
                            + " length " + len + " expected " + fileSize);
                }
            }
            if (SysProperties.CHECK2 && len % Constants.FILE_BLOCK_SIZE != 0) {
                long newLength = len + Constants.FILE_BLOCK_SIZE - (len % Constants.FILE_BLOCK_SIZE);
                file.truncate(newLength);
                fileSize = newLength;
                DbException.throwInternalError("unaligned file length " + fileName + " len " + len);
            }
            return len;
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
    }

    /**
     * Get the current location of the file pointer.
     *
     * @return the location
     */
    public long getFilePointer() {
        if (SysProperties.CHECK2) {
            try {
                if (file.position() != filePos) {
                    DbException.throwInternalError();
                }
            } catch (IOException e) {
                throw DbException.convertIOException(e, fileName);
            }
        }
        return filePos;
    }

    /**
     * Automatically delete the file once it is no longer in use.
     */
    public void autoDelete() {
        if (autoDeleteReference == null) {
            autoDeleteReference = tempFileDeleter.addFile(fileName, this);
        }
    }

    /**
     * No longer automatically delete the file once it is no longer in use.
     */
    public void stopAutoDelete() {
        tempFileDeleter.stopAutoDelete(autoDeleteReference, fileName);
        autoDeleteReference = null;
    }

    /**
     * Close the file. The file may later be re-opened using openFile.
     */
    public void closeFile() throws IOException {
        file.close();
        file = null;
    }

    /**
     * Just close the file, without setting the reference to null. This method
     * is called when writing failed. The reference is not set to null so that
     * there are no NullPointerExceptions later on.
     */
    private void closeFileSilently() {
        try {
            file.close();
        } catch (IOException e) {
            // ignore
        }
    }

    /**
     * Close the file without throwing any exceptions. Exceptions are simply ignored.
     */
    public void closeSilently() {
        try {
            close();
        } catch (Exception e) {
            // ignore
        }
    }

    /**
     * Close the file (ignoring exceptions) and delete the file.
     */
    public void closeAndDeleteSilently() {
        if (file != null) {
            closeSilently();
            tempFileDeleter.deleteFile(autoDeleteReference, fileName);
            fileName = null;
        }
    }

    /**
     * Close this file.
     */
    public void close() {
        try {
            trace("close", fileName, file);
            if (fileLock != null) {
                fileLock.release();
                fileLock = null;
            }
            if (file != null)
                file.close();
        } catch (Exception e) {
            throw newISE(DataUtils.ERROR_WRITING_FAILED, "Closing failed for file {0}", fileName, e);
        } finally {
            file = null;
        }
    }

    /**
     * Flush all changes.
     */
    public void sync() {
        try {
            file.force(true);
        } catch (IOException e) {
            closeFileSilently();
            throw newISE(DataUtils.ERROR_WRITING_FAILED, "Could not sync file {0}", fileName, e);
        }
    }

    /**
     * Re-open the file. The file pointer will be reset to the previous
     * location.
     */
    public void openFile() throws IOException {
        if (file == null) {
            file = FileUtils.open(fileName, mode);
            file.position(filePos);
        }
    }

    private static void trace(String method, String fileName, Object o) {
        if (SysProperties.TRACE_IO) {
            System.out.println("FileStorage." + method + " " + fileName + " " + o);
        }
    }

    /**
     * Read from the file.
     *
     * @param pos the write position
     * @param len the number of bytes to read
     * @return the byte buffer
     */
    public ByteBuffer readFully(long pos, int len) {
        ByteBuffer dst = ByteBuffer.allocate(len);
        if (len > 0) {
            DataUtils.readFully(file, pos, dst);
            readCount++;
            readBytes += len;
        }
        return dst;
    }

    /**
     * Write to the file.
     *
     * @param pos the write position
     * @param src the source buffer
     */
    public void writeFully(long pos, ByteBuffer src) {
        int len = src.remaining();
        fileSize = Math.max(fileSize, pos + len);
        DataUtils.writeFully(file, pos, src);
        writeCount++;
        writeBytes += len;
    }

    /**
     * Truncate the file.
     *
     * @param size the new file size
     */
    public void truncate(long size) {
        try {
            writeCount++;
            file.truncate(size);
            fileSize = Math.min(fileSize, size);
        } catch (IOException e) {
            throw newISE(DataUtils.ERROR_WRITING_FAILED, //
                    "Could not truncate file {0} to size {1}", fileName, size, e);
        }
    }

    /**
     * Get the file size.
     *
     * @return the file size
     */
    public long size() {
        return fileSize;
    }

    /**
     * Get the file instance in use.
     * <p>
     * The application may read from the file (for example for online backup),
     * but not write to it or truncate it.
     *
     * @return the file
     */
    public FileChannel getFile() {
        return file;
    }

    /**
     * Get the encrypted file instance, if encryption is used.
     * <p>
     * The application may read from the file (for example for online backup),
     * but not write to it or truncate it.
     *
     * @return the encrypted file, or null if encryption is not used
     */
    public FileChannel getEncryptedFile() {
        return encryptedFile;
    }

    public String getFileName() {
        return fileName;
    }

    public void delete() {
        FileUtils.delete(fileName);
    }

    /**
     * Get the number of write operations since this storage was opened.
     * For file based storages, this is the number of file write operations.
     *
     * @return the number of write operations
     */
    public long getWriteCount() {
        return writeCount;
    }

    /**
     * Get the number of written bytes since this storage was opened.
     *
     * @return the number of write operations
     */
    public long getWriteBytes() {
        return writeBytes;
    }

    /**
     * Get the number of read operations since this storage was opened.
     * For file based storages, this is the number of file read operations.
     *
     * @return the number of read operations
     */
    public long getReadCount() {
        return readCount;
    }

    /**
     * Get the number of read bytes since this storage was opened.
     *
     * @return the number of write operations
     */
    public long getReadBytes() {
        return readBytes;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public String toString() {
        return fileName;
    }

    // 这种方案在可用内存不够时如果文件很大会导致OOM
    // public InputStream getInputStream() {
    // checkPowerOff();
    // int len = (int) fileSize;
    // byte[] bytes = new byte[len];
    // try {
    // file.position(0);
    // FileUtils.readFully(file, ByteBuffer.wrap(bytes, 0, len));
    // file.position(filePos);
    // } catch (IOException e) {
    // throw DbException.convertIOException(e, fileName);
    // }
    // return new ByteArrayInputStream(bytes);
    // }

    public InputStream getInputStream() {
        checkPowerOff();
        return new FileStorageInputStream(this, handler, false, false, true);
    }
}
