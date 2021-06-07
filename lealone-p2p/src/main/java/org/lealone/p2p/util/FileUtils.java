/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.util;

import java.io.Closeable;
import java.io.DataInput;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.text.DecimalFormat;
import java.util.Arrays;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.JVMStabilityInspector;

import sun.nio.ch.DirectBuffer;

@SuppressWarnings("restriction")
public class FileUtils {
    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);
    private static final double KB = 1024d;
    private static final double MB = 1024 * 1024d;
    private static final double GB = 1024 * 1024 * 1024d;
    private static final double TB = 1024 * 1024 * 1024 * 1024d;

    private static final DecimalFormat df = new DecimalFormat("#.##");
    private static final boolean canCleanDirectBuffers;

    static {
        boolean canClean = false;
        try {
            ByteBuffer buf = ByteBuffer.allocateDirect(1);
            ((DirectBuffer) buf).cleaner().clean();
            canClean = true;
        } catch (Throwable t) {
            JVMStabilityInspector.inspectThrowable(t);
            logger.info("Cannot initialize un-mmaper.  (Are you using a non-Oracle JVM?)  "
                    + "Compacted data files will not be removed promptly.  "
                    + "Consider using an Oracle JVM or using standard disk access mode");
        }
        canCleanDirectBuffers = canClean;
    }

    public static void closeQuietly(Closeable c) {
        try {
            if (c != null)
                c.close();
        } catch (Exception e) {
            logger.warn("Failed closing {}", c, e);
        }
    }

    public static void close(Closeable... cs) throws IOException {
        close(Arrays.asList(cs));
    }

    public static void close(Iterable<? extends Closeable> cs) throws IOException {
        IOException e = null;
        for (Closeable c : cs) {
            try {
                if (c != null)
                    c.close();
            } catch (IOException ex) {
                e = ex;
                logger.warn("Failed closing stream {}", c, ex);
            }
        }
        if (e != null)
            throw e;
    }

    public static boolean isCleanerAvailable() {
        return canCleanDirectBuffers;
    }

    public static void clean(MappedByteBuffer buffer) {
        ((DirectBuffer) buffer).cleaner().clean();
    }

    public static boolean delete(String file) {
        File f = new File(file);
        return f.delete();
    }

    public static void delete(File... files) {
        for (File file : files) {
            file.delete();
        }
    }

    public static File createTempFile(String prefix, String suffix, File directory) {
        try {
            return File.createTempFile(prefix, suffix, directory);
        } catch (IOException e) {
            throw new FSWriteError(e, directory);
        }
    }

    public static File createTempFile(String prefix, String suffix) {
        return createTempFile(prefix, suffix, new File(System.getProperty("java.io.tmpdir")));
    }

    public synchronized static String stringifyFileSize(double value) {
        double d;
        if (value >= TB) {
            d = value / TB;
            String val = df.format(d);
            return val + " TB";
        } else if (value >= GB) {
            d = value / GB;
            String val = df.format(d);
            return val + " GB";
        } else if (value >= MB) {
            d = value / MB;
            String val = df.format(d);
            return val + " MB";
        } else if (value >= KB) {
            d = value / KB;
            String val = df.format(d);
            return val + " KB";
        } else {
            String val = df.format(value);
            return val + " bytes";
        }
    }

    public static void skipBytesFully(DataInput in, int bytes) throws IOException {
        int n = 0;
        while (n < bytes) {
            int skipped = in.skipBytes(bytes - n);
            if (skipped == 0)
                throw new EOFException("EOF after " + n + " bytes out of " + bytes);
            n += skipped;
        }
    }

    /**
     * Get the size of a directory in bytes
     * @param directory The directory for which we need size.
     * @return The size of the directory
     */
    public static long folderSize(File directory) {
        long length = 0;
        for (File file : directory.listFiles()) {
            if (file.isFile())
                length += file.length();
            else
                length += folderSize(file);
        }
        return length;
    }

    public static void copyTo(DataInput in, OutputStream out, int length) throws IOException {
        byte[] buffer = new byte[64 * 1024];
        int copiedBytes = 0;

        while (copiedBytes + buffer.length < length) {
            in.readFully(buffer);
            out.write(buffer);
            copiedBytes += buffer.length;
        }

        if (copiedBytes < length) {
            int left = length - copiedBytes;
            in.readFully(buffer, 0, left);
            out.write(buffer, 0, left);
        }
    }
}
