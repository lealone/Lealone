/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;

public class FileUtils {

    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);
    private static final double KB = 1024d;
    private static final double MB = 1024 * 1024d;
    private static final double GB = 1024 * 1024 * 1024d;
    private static final double TB = 1024 * 1024 * 1024 * 1024d;

    private static final DecimalFormat df = new DecimalFormat("#.##");

    public static void closeQuietly(Closeable c) {
        try {
            if (c != null)
                c.close();
        } catch (Exception e) {
            logger.warn("Failed closing {}", c, e);
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

    public synchronized static String stringifySize(double value) {
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
}
