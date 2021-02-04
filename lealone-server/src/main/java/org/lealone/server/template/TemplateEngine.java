/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.server.template;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
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
        return processRecursive(templateFile, null, false);
    }

    private String processRecursive(String templateFile, Element element, boolean replace) throws Exception {
        String str = readTemplateFile(templateFile);
        if (str.indexOf("v-replace") == -1 && str.indexOf("v-insert") == -1) {
            if (element != null) {
                if (replace) {
                    Document doc = Jsoup.parse(str);
                    for (Node n : doc.body().childNodes()) {
                        if (n instanceof Element) {
                            element.replaceWith(n);
                            break;
                        }
                    }
                } else {
                    element.append(str);
                }
            }
            return str;
        }

        Document doc = Jsoup.parse(str);
        Elements elements = doc.select("[v-replace]");
        for (Element e : elements) {
            processRecursive(e.attr("v-replace"), e, true);
        }

        elements = doc.select("[v-insert]");
        for (Element e : elements) {
            String f = e.attr("v-insert");
            e.removeAttr("v-insert");
            processRecursive(f, e, false);
        }

        if (element != null) {
            for (Node n : doc.body().childNodes()) {
                if (n instanceof Element) {
                    if (replace)
                        element.replaceWith(n);
                    else
                        element.appendChild(n);
                    break;
                }
            }
            return null;
        }
        return doc.html();
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
            throw new RuntimeException("Invalid template file: " + templateFile + ", template root: " + templateRoot);
        return templatePath;
    }
}
