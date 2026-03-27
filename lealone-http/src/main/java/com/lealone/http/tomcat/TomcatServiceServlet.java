/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.http.tomcat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.orm.json.JsonObject;
import com.lealone.service.ServiceHandler;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class TomcatServiceServlet extends HttpServlet {

    private final ServiceHandler serviceHandler;

    public TomcatServiceServlet(ServiceHandler serviceHandler) {
        this.serviceHandler = serviceHandler;
    }

    public String executeService(HttpServletRequest request, HttpServletResponse response,
            boolean disableDynamicCompile) throws IOException {
        // 不能用getRequestURI()，因为它包含Context路径
        String url = request.getServletPath();
        String[] a = url.split("/");
        if (a.length < 4) {
            throw new RuntimeException("service " + url + " not found");
        }
        String serviceName = a[2];
        String methodName = a[3];

        CaseInsensitiveMap<Object> methodArgs = getMethodArgs(request);
        return serviceHandler.executeService(serviceName, methodName, methodArgs, disableDynamicCompile);
    }

    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String url = request.getRequestURI();
        String[] a = url.split("/");
        if (a.length < 4) {
            response.sendError(400, "service " + url + " not found");
            return;
        }
        String serviceName = a[2];
        String methodName = a[3];

        CaseInsensitiveMap<Object> methodArgs = getMethodArgs(request);
        String result = serviceHandler.executeService(serviceName, methodName, methodArgs);
        sendHttpServiceResponse(response, serviceName, methodName, result);
    }

    protected static CaseInsensitiveMap<Object> getMethodArgs(HttpServletRequest request) {
        return getMethodArgs(request, true);
    }

    protected static CaseInsensitiveMap<Object> getMethodArgs(HttpServletRequest request,
            boolean parseJson) {
        Map<String, String[]> map = request.getParameterMap();
        CaseInsensitiveMap<Object> methodArgs = new CaseInsensitiveMap<>();
        // 当请求头包含content-type: application/json时，客户端发送的是一个json类型的数据
        // map不为空时表示上传文件后不需要再解析post请求体
        if (map.isEmpty() && request.getMethod().equalsIgnoreCase("post")) {
            try {
                BufferedReader reader = new BufferedReader(
                        new java.io.InputStreamReader(request.getInputStream()));
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
                reader.close();
                String jsonStr = sb.toString();
                String type = request.getContentType();
                if (type.startsWith("application/json")) {
                    JsonObject obj = new JsonObject(jsonStr);
                    methodArgs.putAll(obj.getMap());
                }
            } catch (IOException e) {
            }
        } else {
            for (Entry<String, String[]> e : request.getParameterMap().entrySet()) {
                String[] values = e.getValue();
                String key = e.getKey();
                for (int i = 0, len = values.length; i < len; i++) {
                    addMethodArgs(methodArgs, key, values[i]);
                }
            }
        }
        return methodArgs;
    }

    protected void sendHttpServiceResponse(HttpServletResponse response, String serviceName,
            String methodName, String result) throws IOException {
        response.setCharacterEncoding("UTF-8");
        response.setContentType("application/json; charset=utf-8");
        response.setHeader("Server", "Lealone Embedded Tomcat");
        response.setHeader("Access-Control-Allow-Origin", "*");
        try (Writer writer = response.getWriter()) {
            writer.write(result);
            writer.flush();
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
}
