/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.template;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.storage.fs.FileUtils;

public class TemplateCompiler {

    private static final Logger logger = LoggerFactory.getLogger(TemplateCompiler.class);

    public static void main(String[] args) throws IOException {
        TemplateCompiler compiler = new TemplateCompiler();
        compiler.parseArgs(args);
        compiler.compile();
    }

    private String webRoot = "./web";
    private String templateDirName = "template";
    private String targetDir = "./target";
    private String charsetName = "utf-8";
    private TemplateEngine te;

    private void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            args[i] = args[i].trim();
        }

        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            switch (a) {
            case "-webRoot":
                webRoot = args[++i];
                break;
            case "-targetDir":
                targetDir = args[++i];
                break;
            case "-templateDirName":
                templateDirName = args[++i];
                break;
            case "-charsetName":
                charsetName = args[++i];
                break;
            default:
                logger.warn("Invalid option: '" + a + "'");
                System.exit(-1);
            }
        }
        te = new TemplateEngine(webRoot, charsetName);
    }

    private void compile() throws IOException {
        File webRoot = new File(this.webRoot);
        File targetDir = new File(this.targetDir, webRoot.getName());
        if (targetDir.exists()) {
            FileUtils.deleteRecursive(targetDir.getAbsolutePath(), true);
            targetDir.mkdir(); // 只创建子目录
        } else {
            targetDir.mkdirs(); // 创建子目录和父目录
        }

        for (File f : webRoot.listFiles()) {
            compileRecursive(targetDir, f);
        }
    }

    private void compileRecursive(File targetDir, File file) throws IOException {
        String fileName = file.getName();
        // 跳过eclipse生成的文件
        if (fileName.startsWith("."))
            return;
        if (file.isDirectory()) {
            if (fileName.equals(templateDirName))
                return;
            targetDir = new File(targetDir, fileName);
            targetDir.mkdir();
            for (File f : file.listFiles()) {
                compileRecursive(targetDir, f);
            }
        } else {
            fileName = fileName.toLowerCase();
            File outFile = new File(targetDir, fileName);
            if (fileName.endsWith(".html")) {
                logger.info("compile file: " + file.getCanonicalPath() + ", to: "
                        + outFile.getCanonicalPath());
                try (OutputStream out = new BufferedOutputStream(new FileOutputStream(outFile))) {
                    String str = te.process(file.getAbsolutePath());
                    out.write(str.getBytes(charsetName));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                try (InputStream in = new BufferedInputStream(new FileInputStream(file));
                        OutputStream out = new BufferedOutputStream(new FileOutputStream(outFile))) {
                    int n = 0;
                    byte[] buffer = new byte[4096];
                    while (-1 != (n = in.read(buffer))) {
                        out.write(buffer, 0, n);
                    }
                }
            }
        }
    }
}
