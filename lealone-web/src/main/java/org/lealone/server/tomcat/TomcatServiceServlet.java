/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.tomcat;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.server.service.ServiceHandler;

public class TomcatServiceServlet extends HttpServlet {

    private final ServiceHandler serviceHandler;

    public TomcatServiceServlet(ServiceHandler serviceHandler) {
        this.serviceHandler = serviceHandler;
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
        CaseInsensitiveMap<Object> methodArgs = new CaseInsensitiveMap<>();

        for (Entry<String, String[]> e : request.getParameterMap().entrySet()) {
            String[] values = e.getValue();
            String key = e.getKey();
            for (int i = 0, len = values.length; i < len; i++) {
                addMethodArgs(methodArgs, key, values[i]);
            }
        }

        // TODO 当请求头包含content-type: application/json时，客户端发送的是一个json类型的数据
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
