/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.template;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.lealone.common.util.IOUtils;

public class TemplateEngine {

    private final String templateRoot;
    private final String charsetName;

    public TemplateEngine(String templateRoot, String charsetName) {
        try {
            templateRoot = new File(templateRoot).getCanonicalPath();
        } catch (IOException e) {
            throw new RuntimeException("Invalid templateRoot: " + templateRoot);
        }
        this.templateRoot = templateRoot;
        this.charsetName = charsetName;
    }

    public String process(String templateFile) throws Exception {
        return processRecursive(templateFile);
    }

    private String processRecursive(String templateFile) throws Exception {
        char[] chars = readTemplateFile(templateFile).toCharArray();
        int i = 0;
        int len = chars.length;
        StringBuilder buff = new StringBuilder(len);
        while (i < len) {
            char c = chars[i++];
            switch (c) {
            case '<': {
                // 跳过注释
                if (chars[i] == '!' && chars[i + 1] == '-' && chars[i + 2] == '-') {
                    i += 3;
                    while (i < len) {
                        c = chars[i++];
                        if (c == '-' && chars[i] == '-' && chars[i + 1] == '>') {
                            i += 2;
                            while (i < len) {
                                c = chars[i];
                                if (c != '\r' && c != '\n')
                                    break;
                                i++;
                            }
                            break;
                        }
                        continue;
                    }
                } else if (chars[i] == 't' && chars[i + 1] == 'e' && chars[i + 2] == 'm'
                        && chars[i + 3] == 'p' && chars[i + 4] == 'l' && chars[i + 5] == 'a'
                        && chars[i + 6] == 't' && chars[i + 7] == 'e') {
                    i += 8;
                    boolean isTemplate = false;
                    int templateStart = i;
                    int templateEnd = 0;
                    while (i < len) {
                        c = chars[i++];
                        if (c == '/' && chars[i] == '>') {
                            templateEnd = i;
                            i += 1;
                            isTemplate = true;
                            break;
                        } else if (c == '>') {
                            while (i < len) {
                                c = chars[i++];
                                if (!(c == ' ' || c == '\t' || c == '\r' || c == '\n'))
                                    break;
                            }
                            if (c == '<' && chars[i] == '/' && chars[i + 1] == 't' && chars[i + 2] == 'e'
                                    && chars[i + 3] == 'm' && chars[i + 4] == 'p' && chars[i + 5] == 'l'
                                    && chars[i + 6] == 'a' && chars[i + 7] == 't' && chars[i + 8] == 'e'
                                    && chars[i + 9] == '>') {
                                templateEnd = i;
                                i += 10;
                                isTemplate = true;
                                break;
                            } else {
                                buff.append(new String(chars, templateStart - 9, i - templateStart + 9));
                                isTemplate = false;
                                break;
                            }
                        }
                    }
                    if (isTemplate) {
                        String t = parseTemplateElement(chars, templateStart, templateEnd - 1);
                        buff.append(t);
                    }
                    continue;
                } else {
                    buff.append(c);
                }
                break;
            }
            default:
                buff.append(c);
            }
        }
        return buff.toString();
    }

    private String parseTemplateElement(char[] chars, int startIndex, int endIndex) throws Exception {
        StringBuilder buff = new StringBuilder("<template");
        StringBuilder buff2 = new StringBuilder();
        int i = startIndex;
        boolean isInsert = false;
        char c;
        while (i < endIndex) {
            c = chars[i++];
            switch (c) {
            case 'v': {
                if (chars[i] == '-') {
                    if (chars[i + 1] == 'r' && chars[i + 2] == 'e' && chars[i + 3] == 'p'
                            && chars[i + 4] == 'l' && chars[i + 5] == 'a' && chars[i + 6] == 'c'
                            && chars[i + 7] == 'e') {
                        i += 8;
                        parseTemplateFile(chars, i, endIndex, buff2);
                        return buff2.toString();
                    } else if (!isInsert && chars[i + 1] == 'i' && chars[i + 2] == 'n'
                            && chars[i + 3] == 's' && chars[i + 4] == 'e' && chars[i + 5] == 'r'
                            && chars[i + 6] == 't') {
                        i += 7;
                        isInsert = true;
                        i = parseTemplateFile(chars, i, endIndex, buff2);
                        continue;
                    } else {
                        buff.append(c);
                    }
                } else {
                    buff.append(c);
                }
                break;
            }
            default:
                buff.append(c);
            }
        }
        if (isInsert) {
            while (buff.lastIndexOf(" ") == (buff.length() - 1))
                buff.setLength(buff.length() - 1);
            buff.append(">\n");
        }
        buff.append(buff2);
        buff.append("</template>");
        return buff.toString();
    }

    private int parseTemplateFile(char[] chars, int startIndex, int endIndex, StringBuilder buff)
            throws Exception {
        int i = startIndex;
        char c;
        while (i < endIndex) {
            c = chars[i++];
            if (c == '\'' || c == '"')
                break;
        }
        int templateStart = i;
        while (i < endIndex) {
            c = chars[i++];
            if (c == '\'' || c == '"')
                break;
        }
        int templateEnd = i;
        String templateFile = new String(chars, templateStart, templateEnd - templateStart - 1);
        String str = processRecursive(templateFile);
        buff.append(str);
        return i;
    }

    private String readTemplateFile(String templateFile) throws Exception {
        templateFile = getCanonicalTemplateFile(templateFile);
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(templateFile), charsetName));
        return IOUtils.readStringAndClose(reader, -1);
    }

    private String getCanonicalTemplateFile(String templateFile) {
        String templatePath;
        try {
            // 模板文件的位置总是在templateRoot之下
            templatePath = new File(templateFile).getCanonicalPath();
            if (!templatePath.startsWith(templateRoot)) {
                templatePath = new File(templateRoot, templateFile).getCanonicalPath();
                if (!templatePath.startsWith(templateRoot)) {
                    templatePath = null;
                }
            }
        } catch (IOException e) {
            templatePath = null;
        }
        if (templatePath == null)
            throw new RuntimeException(
                    "Invalid template file: " + templateFile + ", template root: " + templateRoot);
        return templatePath;
    }
}
