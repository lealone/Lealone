/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.http.jdk;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.orm.json.JsonObject;
import com.lealone.service.ServiceHandler;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class JdkHttpServiceHandler implements HttpHandler {

    private final ServiceHandler serviceHandler;

    public JdkHttpServiceHandler(ServiceHandler serviceHandler) {
        this.serviceHandler = serviceHandler;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        URI uri = exchange.getRequestURI();
        String url = uri.getPath();
        String[] a = url.split("/");
        if (a.length < 4) {
            byte[] respContents = ("service " + url + " not found").getBytes("UTF-8");
            exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");
            exchange.sendResponseHeaders(400, respContents.length);
            exchange.getResponseBody().write(respContents);
            exchange.close();
            return;
        }
        String serviceName = a[2];
        String methodName = a[3];
        CaseInsensitiveMap<Object> methodArgs = getMethodArgs(exchange);
        String result = serviceHandler.executeService(serviceName, methodName, methodArgs);
        sendHttpServiceResponse(exchange, result);
    }

    private static CaseInsensitiveMap<Object> getMethodArgs(HttpExchange exchange) throws IOException {
        CaseInsensitiveMap<Object> params = new CaseInsensitiveMap<>();
        String method = exchange.getRequestMethod();
        // 1. 解析 GET 参数
        if ("GET".equalsIgnoreCase(method)) {
            String query = exchange.getRequestURI().getQuery();
            if (query != null)
                parseQuery(query, params);
        }
        // 2. 解析 POST 表单参数
        else if ("POST".equalsIgnoreCase(method)) {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            String type = exchange.getRequestHeaders().getFirst("content-type");
            // 当请求头包含content-type: application/json时，客户端发送的是一个json类型的数据
            // map不为空时表示上传文件后不需要再解析post请求体
            if (type != null && type.toLowerCase().startsWith("application/json")) {
                JsonObject obj = new JsonObject(body);
                params.putAll(obj.getMap());
            } else {
                parseQuery(body, params);
            }
        }
        return params;
    }

    // 通用解析 key=value&key2=value2
    private static void parseQuery(String query, CaseInsensitiveMap<Object> params) {
        for (String pair : query.split("&")) {
            String[] kv = pair.split("=", 2);
            if (kv.length == 2) {
                String key = kv[0];
                String value = URLDecoder.decode(kv[1], StandardCharsets.UTF_8);
                addMethodArgs(params, key, value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void addMethodArgs(CaseInsensitiveMap<Object> methodArgs, String key, String value) {
        Object oldValue = methodArgs.get(key);
        if (oldValue != null) {
            List<String> list;
            if (oldValue instanceof String) {
                list = new ArrayList<String>();
                list.add((String) oldValue);
                methodArgs.put(key, list);
            } else {
                list = (List<String>) oldValue;
            }
            list.add(value);
        } else {
            methodArgs.put(key, value);
        }
    }

    private static void sendHttpServiceResponse(HttpExchange exchange, String result)
            throws IOException {
        byte[] respContents = result.getBytes("UTF-8");
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        exchange.getResponseHeaders().add("Server", "Lealone HttpServer");
        exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        exchange.sendResponseHeaders(200, respContents.length);
        exchange.getResponseBody().write(respContents);
    }
}
