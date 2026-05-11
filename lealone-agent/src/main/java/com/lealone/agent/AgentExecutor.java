/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.agent;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.common.util.IOUtils;
import com.lealone.common.util.MapUtils;
import com.lealone.common.util.StringUtils;
import com.lealone.db.Constants;
import com.lealone.db.Database;
import com.lealone.db.SysProperties;
import com.lealone.db.plugin.PluginManager;
import com.lealone.db.session.ServerSession;
import com.lealone.db.util.SourceCompiler;
import com.lealone.server.ProtocolServerEngine;

public class AgentExecutor {

    private static final Logger logger = LoggerFactory.getLogger(AgentExecutor.class);

    private final String userPrompt;
    private final Database db;
    private final ServerSession session;

    public AgentExecutor(String userPrompt, Database db, ServerSession session) {
        this.userPrompt = userPrompt;
        this.db = db;
        this.session = session;
    }

    public String execute() {
        long t1 = System.currentTimeMillis();
        String systemPrompt = """
                                \n\n以上是用户的需求，请从后面的服务列表中找出可能用到的服务，并严格遵循以下规范,3选1:
                1.只要用到lealone服务，就用 create table 定义表，用 create service 定义服务接口
                create service 的格式如下:
                create service 服务名(
                  方法名(参数名 sql类型) 返回的sql类型，
                )
                sql类型可以是表名，如果返回类型是一批记录就用list<表名>,
                回答的格式是: 1:<用户的应用英文名>:<create table和create service语句>
                不要```sql标记，不要返回其他的。

                2.如果存在其他可用的服务就必需基于这些服务用java实现一个完整的类，
                核心代码放在public static String run()中实现，如果是命令行工具要用ProcessBuilder执行，
                回答的格式是: <逗号分隔的name列表>:<类名>:<java代码>
                不要包含java标记也不需要注释。

                3.如果没有合适的服务就返回0和一个冒号然后你自由回答。

                以下是可用的服务,按name:comment的方式排列:
                                """;
        StringBuilder prompt = new StringBuilder(userPrompt);
        prompt.append(systemPrompt);
        Map<String, Map<String, String>> extServices = db.getExternalService().getRecords(session);
        for (Map<String, String> map : extServices.values()) {
            prompt.append(map.get("name")).append(":").append(map.get("comment")).append("\n");
        }
        if (!extServices.containsKey("lealone")) {
            prompt.append("lealone:氛围编程或开发企业应用\n");
        }
        if (db.isPromptMode()) {
            logger.info("Prompt:\n{}", prompt);
            return prompt.toString();
        }
        AtomicReference<String> previousResponseId = new AtomicReference<>();
        CodeAgent agent = db.getCodeAgent();
        String content = agent.send(prompt.toString(), previousResponseId);
        if (content.startsWith("0:")) {
            content = content.substring(2);
        } else if (content.startsWith("1:")) {
            content = content.substring(2);
            int pos = content.indexOf(':');
            String appName = content.substring(0, pos);
            content = content.substring(pos + 1);
            content = vibeCoding(agent, content, appName, previousResponseId);
        } else {
            int pos = content.indexOf(':');
            if (pos >= 0) {
                File lib = new File(SysProperties.getBaseDir(), "lib");
                String nameStr = content.substring(0, pos);
                String code = content.substring(pos + 1);
                pos = code.indexOf(':');
                String className = code.substring(0, pos);
                code = code.substring(pos + 1);
                String[] names = StringUtils.arraySplit(nameStr, ',');
                ArrayList<URL> urls = new ArrayList<>();
                for (String name : names) {
                    Map<String, String> map = extServices.get(name);
                    String url = map.get("url");
                    if (url.equalsIgnoreCase("cli"))
                        continue;
                    try {
                        File f = new File(url);
                        if (f.exists()) {
                            urls.add(f.toURI().toURL());
                            continue;
                        }
                    } catch (Exception e) {
                    }
                    File jarFile;
                    pos = url.lastIndexOf('/');
                    if (pos >= 0) {
                        jarFile = new File(lib, url.substring(pos + 1));
                    } else {
                        continue;
                    }
                    try {
                        if (!jarFile.exists()) {
                            URI uri = URI.create(url);
                            downloadJar(uri, lib.getAbsolutePath());
                        }
                        urls.add(jarFile.toURI().toURL());
                    } catch (Exception e) {
                    }
                }
                SourceCompiler compiler = new SourceCompiler();
                compiler.setSource(className, code);
                compiler.setUrls(urls.toArray(new URL[0]));
                try {
                    Method mainMethod = compiler.compile(className).getMethod("run");
                    content = (String) mainMethod.invoke(null);
                } catch (Exception e) {
                    throw DbException.convert(e);
                }
            }
        }
        logger.info("Agent execute time: " + (System.currentTimeMillis() - t1) + " ms");
        return content;
    }

    private String vibeCoding(CodeAgent agent, String sql, String appName,
            AtomicReference<String> previousResponseId) {
        String userPrompt = """
                按照服务接口生成完整的html代码，前端用“/service/服务名/方法名”这种格式调用后端服务，
                只返回html代码，不要```html标记，不要返回其他的。
                       """;
        logger.info("Agent execute sql: " + sql);
        session.createNestedSession().executeUpdateLocal(sql);
        String content = agent.send(userPrompt, previousResponseId);
        ProtocolServerEngine pse = PluginManager.getPlugin(ProtocolServerEngine.class, "HTTP");
        String webRoot = MapUtils.getString(pse.getConfig(), "web_root", "./web");
        String host = MapUtils.getString(pse.getConfig(), "host", Constants.DEFAULT_HOST);
        int port = MapUtils.getInt(pse.getConfig(), "port", Constants.DEFAULT_TCP_PORT);
        File htmlFile = new File(webRoot, appName + ".html");
        try {
            Charset utf8 = Charset.forName("UTF-8");
            BufferedOutputStream file = new BufferedOutputStream(new FileOutputStream(htmlFile));
            file.write(content.toString().getBytes(utf8));
            file.close();
        } catch (IOException e) {
            throw DbException.convertIOException(e, "Failed to write html file: " + htmlFile);
        }
        return "http://" + host + ":" + port + "/" + appName + ".html";
    }

    private static void downloadJar(URI uri, String savePath) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(10000); // 连接超时 10 秒
        conn.setReadTimeout(10000); // 读取超时 10 秒

        // 获取响应码，判断是否下载成功
        int responseCode = conn.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new IOException("下载失败，HTTP 响应码：" + responseCode);
        }
        try (InputStream inputStream = conn.getInputStream();
                FileOutputStream outputStream = new FileOutputStream(savePath);
                BufferedInputStream bufferedIn = new BufferedInputStream(inputStream)) {
            IOUtils.copy(bufferedIn, outputStream);
        } finally {
            // 断开连接
            conn.disconnect();
        }
    }
}
