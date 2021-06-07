/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.misc;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ZipFileCopyTest {

    public static void main(String[] args) throws Exception {
        String zipFile = args[0];
        String outDir = args[1];
        run(zipFile, outDir);
    }

    public static void run(String zipFile, String outDir) throws Exception {
        ZipFile zf = new ZipFile(zipFile);
        new File(outDir).mkdirs();
        Enumeration<? extends ZipEntry> e = zf.entries();
        while (e.hasMoreElements()) {
            ZipEntry ze = e.nextElement();
            if (!ze.isDirectory()) {
                String name = ze.getName();
                System.out.println(name);
                InputStream in = zf.getInputStream(ze);

                int pos = name.lastIndexOf('/');
                if (pos < 0) {
                    pos = name.lastIndexOf('\\');
                }
                if (pos > 0) {
                    name = name.substring(pos + 1);
                }
                String outFile = outDir + "/" + name;

                OutputStream out = new FileOutputStream(outFile);
                int n = 0;
                byte[] buffer = new byte[4096];
                while (-1 != (n = in.read(buffer))) {
                    out.write(buffer, 0, n);
                }
                in.close();
                out.close();
            } else {
                String outFile = outDir + "/" + ze.getName();
                new File(outFile).mkdir();
            }
        }
        zf.close();
    }
}
