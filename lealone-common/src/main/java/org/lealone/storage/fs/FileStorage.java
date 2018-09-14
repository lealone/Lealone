/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.storage.fs;

import java.io.IOException;
import java.lang.ref.Reference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Arrays;
import java.util.Map;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.security.SecureFileStorage;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.TempFileDeleter;
import org.lealone.db.Constants;
import org.lealone.db.DataHandler;
import org.lealone.db.SysProperties;
import org.lealone.db.api.ErrorCode;
import org.lealone.storage.cache.FilePathCache;

/**
 * This class is an abstraction of a random access file.
 * Each file contains a magic header, and reading / writing is done in blocks.
 * See also {@link SecureFileStorage}
 */
public class FileStorage {

    /**
     * The size of the file header in bytes.
     */
    public static final int HEADER_LENGTH = 3 * Constants.FILE_BLOCK_SIZE;

    /**
     * The magic file header.
     */
    private static final String HEADER = "-- H2 0.5/B --      ".substring(0, Constants.FILE_BLOCK_SIZE - 1) + "\n";

    /**
     * The file name.
     */
    protected String name;

    /**
     * The callback object is responsible to check access rights, and free up
     * disk space if required.
     */
    private DataHandler handler;

    private long filePos;
    private long fileLength;
    private Reference<?> autoDeleteReference;
    private boolean checkedWriting = true;
    private String mode;
    private TempFileDeleter tempFileDeleter;
    private java.nio.channels.FileLock lock;

    /**
     * The number of read operations.
     */
    protected long readCount;

    /**
     * The number of read bytes.
     */
    protected long readBytes;

    /**
     * The number of write operations.
     */
    protected long writeCount;

    /**
     * The number of written bytes.
     */
    protected long writeBytes;

    /**
     * The file name.
     */
    protected String fileName;

    /**
     * Whether this storage is read-only.
     */
    protected boolean readOnly;

    /**
     * The file size (cached).
     */
    protected long fileSize;

    /**
     * The file.
     */
    protected FileChannel file;

    /**
     * The encrypted file (if encryption is used).
     */
    protected FileChannel encryptedFile;

    /**
     * The file lock.
     */
    protected FileLock fileLock;

    @Override
    public String toString() {
        return fileName;
    }

    public FileStorage() {

    }

    /**
     * Create a new file using the given settings.
     *
     * @param handler the callback object
     * @param name the file name
     * @param mode the access mode ("r", "rw", "rws", "rwd")
     */
    public FileStorage(DataHandler handler, String name, String mode) {
        this.handler = handler;
        this.name = name;
        if (handler != null) {
            tempFileDeleter = handler.getTempFileDeleter();
        } else {
            tempFileDeleter = null;
        }
        try {
            boolean exists = FileUtils.exists(name);
            if (exists && !FileUtils.canWrite(name)) {
                mode = "r";
            } else {
                FileUtils.createDirectories(FileUtils.getParent(name));
            }
            file = FileUtils.open(name, mode);
            if (exists) {
                fileLength = file.size();
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, "name: " + name + " mode: " + mode);
        }
        this.mode = mode;
    }

    /**
     * Open a non encrypted file store with the given settings.
     *
     * @param handler the data handler
     * @param name the file name
     * @param mode the access mode (r, rw, rws, rwd)
     * @return the created object
     */
    public static FileStorage open(DataHandler handler, String name, String mode) {
        return open(handler, name, mode, null, null, 0);
    }

    /**
     * Open an encrypted file store with the given settings.
     *
     * @param handler the data handler
     * @param name the file name
     * @param mode the access mode (r, rw, rws, rwd)
     * @param cipher the name of the cipher algorithm
     * @param key the encryption key
     * @return the created object
     */
    public static FileStorage open(DataHandler handler, String name, String mode, String cipher, byte[] key) {
        return open(handler, name, mode, cipher, key, Constants.ENCRYPTION_KEY_HASH_ITERATIONS);
    }

    /**
     * Open an encrypted file store with the given settings.
     *
     * @param handler the data handler
     * @param name the file name
     * @param mode the access mode (r, rw, rws, rwd)
     * @param cipher the name of the cipher algorithm
     * @param key the encryption key
     * @param keyIterations the number of iterations the key should be hashed
     * @return the created object
     */
    public static FileStorage open(DataHandler handler, String name, String mode, String cipher, byte[] key,
            int keyIterations) {
        FileStorage store;
        if (cipher == null) {
            store = new FileStorage(handler, name, mode);
        } else {
            store = new SecureFileStorage(handler, name, mode, cipher, key, keyIterations);
        }
        return store;
    }

    /**
     * Generate the random salt bytes if required.
     *
     * @return the random salt or the magic
     */
    protected byte[] generateSalt() {
        return HEADER.getBytes();
    }

    /**
     * Initialize the key using the given salt.
     *
     * @param salt the salt
     */
    protected void initKey(byte[] salt) {
        // do nothing
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
     * Initialize the file. This method will write or check the file header if
     * required.
     */
    public void init() {
        int len = Constants.FILE_BLOCK_SIZE;
        byte[] salt;
        byte[] magic = HEADER.getBytes();
        if (length() < HEADER_LENGTH) {
            // write unencrypted
            checkedWriting = false;
            writeDirect(magic, 0, len);
            salt = generateSalt();
            writeDirect(salt, 0, len);
            initKey(salt);
            // write (maybe) encrypted
            write(magic, 0, len);
            checkedWriting = true;
        } else {
            // read unencrypted
            seek(0);
            byte[] buff = new byte[len];
            readFullyDirect(buff, 0, len);
            if (!Arrays.equals(buff, magic)) {
                throw DbException.get(ErrorCode.FILE_VERSION_ERROR_1, name);
            }
            salt = new byte[len];
            readFullyDirect(salt, 0, len);
            initKey(salt);
            // read (maybe) encrypted
            readFully(buff, 0, Constants.FILE_BLOCK_SIZE);
            if (!Arrays.equals(buff, magic)) {
                throw DbException.get(ErrorCode.FILE_ENCRYPTION_ERROR_1, name);
            }
        }
    }

    // /**
    // * Close the file.
    // */
    // public void close() {
    // if (file != null) {
    // try {
    // trace("close", name, file);
    // file.close();
    // } catch (IOException e) {
    // throw DbException.convertIOException(e, name);
    // } finally {
    // file = null;
    // }
    // }
    // }

    /**
     * Close the file without throwing any exceptions. Exceptions are simply
     * ignored.
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
            tempFileDeleter.deleteFile(autoDeleteReference, name);
            name = null;
        }
    }

    /**
     * Read a number of bytes without decrypting.
     *
     * @param b the target buffer
     * @param off the offset
     * @param len the number of bytes to read
     */
    protected void readFullyDirect(byte[] b, int off, int len) {
        readFully(b, off, len);
    }

    /**
     * Read a number of bytes.
     *
     * @param b the target buffer
     * @param off the offset
     * @param len the number of bytes to read
     */
    public void readFully(byte[] b, int off, int len) {
        if (SysProperties.CHECK && (len < 0 || len % Constants.FILE_BLOCK_SIZE != 0)) {
            DbException.throwInternalError("unaligned read " + name + " len " + len);
        }
        checkPowerOff();
        try {
            FileUtils.readFully(file, ByteBuffer.wrap(b, off, len));
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
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
            DbException.throwInternalError("unaligned seek " + name + " pos " + pos);
        }
        try {
            if (pos != filePos) {
                file.position(pos);
                filePos = pos;
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
        }
    }

    /**
     * Write a number of bytes without encrypting.
     *
     * @param b the source buffer
     * @param off the offset
     * @param len the number of bytes to write
     */
    protected void writeDirect(byte[] b, int off, int len) {
        write(b, off, len);
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
            DbException.throwInternalError("unaligned write " + name + " len " + len);
        }
        checkWritingAllowed();
        checkPowerOff();
        try {
            FileUtils.writeFully(file, ByteBuffer.wrap(b, off, len));
        } catch (IOException e) {
            closeFileSilently();
            throw DbException.convertIOException(e, name);
        }
        filePos += len;
        fileLength = Math.max(filePos, fileLength);
    }

    /**
     * Set the length of the file. This will expand or shrink the file.
     *
     * @param newLength the new file size
     */
    public void setLength(long newLength) {
        if (SysProperties.CHECK && newLength % Constants.FILE_BLOCK_SIZE != 0) {
            DbException.throwInternalError("unaligned setLength " + name + " pos " + newLength);
        }
        checkPowerOff();
        checkWritingAllowed();
        try {
            if (newLength > fileLength) {
                long pos = filePos;
                file.position(newLength - 1);
                FileUtils.writeFully(file, ByteBuffer.wrap(new byte[1]));
                file.position(pos);
            } else {
                file.truncate(newLength);
            }
            fileLength = newLength;
        } catch (IOException e) {
            closeFileSilently();
            throw DbException.convertIOException(e, name);
        }
    }

    /**
     * Get the file size in bytes.
     *
     * @return the file size
     */
    public long length() {
        try {
            long len = fileLength;
            if (SysProperties.CHECK2) {
                len = file.size();
                if (len != fileLength) {
                    DbException.throwInternalError("file " + name + " length " + len + " expected " + fileLength);
                }
            }
            if (SysProperties.CHECK2 && len % Constants.FILE_BLOCK_SIZE != 0) {
                long newLength = len + Constants.FILE_BLOCK_SIZE - (len % Constants.FILE_BLOCK_SIZE);
                file.truncate(newLength);
                fileLength = newLength;
                DbException.throwInternalError("unaligned file length " + name + " len " + len);
            }
            return len;
        } catch (IOException e) {
            throw DbException.convertIOException(e, name);
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
                throw DbException.convertIOException(e, name);
            }
        }
        return filePos;
    }

    /**
     * Call fsync. Depending on the operating system and hardware, this may or
     * may not in fact write the changes.
     */
    // public void sync() {
    // try {
    // file.force(true);
    // } catch (IOException e) {
    // closeFileSilently();
    // throw DbException.convertIOException(e, name);
    // }
    // }

    /**
     * Automatically delete the file once it is no longer in use.
     */
    public void autoDelete() {
        if (autoDeleteReference == null) {
            autoDeleteReference = tempFileDeleter.addFile(name, this);
        }
    }

    /**
     * No longer automatically delete the file once it is no longer in use.
     */
    public void stopAutoDelete() {
        tempFileDeleter.stopAutoDelete(autoDeleteReference, name);
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
     * Re-open the file. The file pointer will be reset to the previous
     * location.
     */
    public void openFile() throws IOException {
        if (file == null) {
            file = FileUtils.open(name, mode);
            file.position(filePos);
        }
    }

    private static void trace(String method, String fileName, Object o) {
        if (SysProperties.TRACE_IO) {
            System.out.println("FileStore." + method + " " + fileName + " " + o);
        }
    }

    /**
     * Try to lock the file.
     *
     * @return true if successful
     */
    public synchronized boolean tryLock() {
        try {
            lock = file.tryLock();
            return lock != null;
        } catch (Exception e) {
            // ignore OverlappingFileLockException
            return false;
        }
    }

    /**
     * Release the file lock.
     */
    public synchronized void releaseLock() {
        if (file != null && lock != null) {
            try {
                lock.release();
            } catch (Exception e) {
                // ignore
            }
            lock = null;
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
     * Try to open the file.
     *
     * @param fileName the file name
     * @param readOnly whether the file should only be opened in read-only mode,
     *            even if the file is writable
     * @param encryptionKey the encryption key, or null if encryption is not
     *            used
     */
    public void open(String fileName, Map<String, ?> config) {
        if (file != null) {
            return;
        }
        char[] encryptionKey = (char[]) config.get("encryptionKey");
        boolean readOnly = config.containsKey("readOnly");

        if (fileName != null) {
            FilePath p = FilePath.get(fileName);
            // if no explicit scheme was specified, NIO is used
            if (p instanceof FilePathDisk && !fileName.startsWith(p.getScheme() + ":")) {
                // ensure the NIO file system is registered
                FilePathNio.class.getName();
                fileName = "nio:" + fileName;
            }
        }
        this.fileName = fileName;
        FilePath f = FilePath.get(fileName);
        FilePath parent = f.getParent();
        if (parent != null && !parent.exists()) {
            throw DataUtils.newIllegalArgumentException("Directory does not exist: {0}", parent);
        }
        if (f.exists() && !f.canWrite()) {
            readOnly = true;
        }
        this.readOnly = readOnly;
        try {
            file = f.open(readOnly ? "r" : "rw");
            if (encryptionKey != null) {
                byte[] key = FilePathEncrypt.getPasswordBytes(encryptionKey);
                encryptedFile = file;
                file = new FilePathEncrypt.FileEncrypt(fileName, key, file);
            }
            file = FilePathCache.wrap(file);
            try {
                if (readOnly) {
                    fileLock = file.tryLock(0, Long.MAX_VALUE, true);
                } else {
                    fileLock = file.tryLock();
                }
            } catch (OverlappingFileLockException e) {
                throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_LOCKED, "The file is locked: {0}",
                        fileName, e);
            }
            if (fileLock == null) {
                throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_LOCKED, "The file is locked: {0}",
                        fileName);
            }
            fileSize = file.size();
        } catch (IOException e) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_READING_FAILED, "Could not open file {0}",
                    fileName, e);
        }
    }

    /**
     * Close this file.
     */
    public void close() {
        try {
            trace("close", name, file);
            if (fileLock != null) {
                fileLock.release();
                fileLock = null;
            }
            if (file != null)
                file.close();
        } catch (Exception e) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED, "Closing failed for file {0}",
                    fileName, e);
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
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED, "Could not sync file {0}",
                    fileName, e);
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
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED,
                    "Could not truncate file {0} to size {1}", fileName, size, e);
        }
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

    /**
     * Get the file name.
     *
     * @return the file name
     */
    public String getFileName() {
        return fileName;
    }

    public void delete() {
        FileUtils.delete(fileName);
    }
}
