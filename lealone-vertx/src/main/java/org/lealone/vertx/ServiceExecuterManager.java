package org.lealone.vertx;

import java.util.HashMap;

import org.lealone.common.util.CamelCaseHelper;

public class ServiceExecuterManager {

    private ServiceExecuterManager() {
    }

    private static final HashMap<String, ServiceExecuter> serviceExecuters = new HashMap<>();

    public synchronized static void registerServiceExecuter(String name, ServiceExecuter serviceExecuter) {
        serviceExecuters.put(name, serviceExecuter);
    }

    public synchronized void deregisterServiceExecuter(String name) {
        serviceExecuters.remove(name);
    }

    public static void executeServiceNoReturnValue(String serviceName, String json) {
        // System.out.println("serviceName=" + serviceName + ", json=" + json);
        int dotPos = serviceName.indexOf('.');
        String methodName = serviceName.substring(dotPos + 1);
        serviceName = serviceName.substring(0, dotPos);
        // serviceName = "org.lealone.test.vertx.impl." + toClassName(serviceName);

        ServiceExecuter serviceExecuter = serviceExecuters.get(serviceName);
        serviceExecuter.executeServiceNoReturnValue(methodName, json);
        //
        // try {
        // Object wrapper = Class.forName(serviceName + "Executer").newInstance();
        // Method m = wrapper.getClass().getMethod("executeServiceNoReturnValue", String.class, String.class);
        // m.invoke(wrapper, methodName, json).toString();
        // } catch (Exception e) {
        // e.printStackTrace();
        // }
    }

    public static String executeServiceWithReturnValue(String serviceName, String json) {
        // System.out.println("serviceName=" + serviceName + ", json=" + json);
        int dotPos = serviceName.indexOf('.');
        String methodName = serviceName.substring(dotPos + 1);
        serviceName = serviceName.substring(0, dotPos);
        // serviceName = "org.lealone.test.vertx.impl." + toClassName(serviceName);

        ServiceExecuter serviceExecuter = serviceExecuters.get(serviceName);
        if (serviceExecuter == null) {
            throw new RuntimeException("service " + serviceName + " not found");
        }
        return serviceExecuter.executeServiceWithReturnValue(methodName, json);

        // try {
        // Object wrapper = Class.forName(serviceName + "Executer").newInstance();
        // Method m = wrapper.getClass().getMethod("executeServiceWithReturnValue", String.class, String.class);
        // json = m.invoke(wrapper, methodName, json).toString();
        // } catch (Exception e) {
        // e.printStackTrace();
        // }
        // return json;
    }

    public static String toClassName(String name) {
        name = CamelCaseHelper.toCamelFromUnderscore(name);
        name = Character.toUpperCase(name.charAt(0)) + name.substring(1);
        return name;
    }

}
