package org.lealone.server.http;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.lealone.common.util.CaseInsensitiveMap;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions;

public class HttpRouterFactory implements RouterFactory {

    @Override
    public Router createRouter(Map<String, String> config, Vertx vertx) {
        Router router = Router.router(vertx);
        initRouter(config, vertx, router);
        setCorsHandler(config, vertx, router);
        setHttpServiceHandler(config, vertx, router);
        setSockJSHandler(router, config, vertx);
        // 放在最后
        setStaticHandler(router, config);
        return router;
    }

    protected void initRouter(Map<String, String> config, Vertx vertx, Router router) {
    }

    protected static CaseInsensitiveMap<Object> getMethodArgs(RoutingContext routingContext) {
        CaseInsensitiveMap<Object> methodArgs = new CaseInsensitiveMap<>();
        for (Map.Entry<String, String> e : routingContext.request().params().entries()) {
            addMethodArgs(methodArgs, e.getKey(), e.getValue());
        }

        // 当请求头包含content-type: application/json时，客户端发送的是一个json类型的数据，
        // BodyHandler只能处理表单类的数据，所以在这里需要从json中取出参数
        if (routingContext.request().method() == HttpMethod.POST) {
            JsonObject json = routingContext.getBodyAsJson();
            if (json != null) {
                for (Map.Entry<String, Object> e : json.getMap().entrySet()) {
                    addMethodArgs(methodArgs, e.getKey(), e.getValue().toString());
                }
            }
        }
        return methodArgs;
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

    protected void setCorsHandler(Map<String, String> config, Vertx vertx, Router router) {
        router.route().handler(CorsHandler.create("*").allowedMethod(HttpMethod.GET).allowedMethod(HttpMethod.POST));
    }

    protected String getServicePath(Map<String, String> config) {
        String servicePath = config.get("service_path");
        if (servicePath == null)
            servicePath = "/service/:serviceName/:methodName";
        return servicePath;
    }

    protected void setHttpServiceHandler(Map<String, String> config, Vertx vertx, Router router) {
        final HttpServiceHandler serviceHandler = new HttpServiceHandler(config);
        String servicePath = getServicePath(config);
        router.post(servicePath).handler(BodyHandler.create());
        router.route(servicePath).handler(routingContext -> {
            handleHttpServiceRequest(serviceHandler, routingContext);
        });
    }

    protected void handleHttpServiceRequest(final HttpServiceHandler serviceHandler, RoutingContext routingContext) {
        String serviceName = routingContext.request().params().get("serviceName");
        String methodName = routingContext.request().params().get("methodName");
        CaseInsensitiveMap<Object> methodArgs = getMethodArgs(routingContext);
        Buffer result = serviceHandler.executeService(serviceName, methodName, methodArgs);
        sendHttpServiceResponse(routingContext, serviceName, methodName, result);
    }

    protected void sendHttpServiceResponse(RoutingContext routingContext, String serviceName, String methodName,
            Buffer result) {
        routingContext.response().putHeader("content-type", "application/json; charset=utf-8");
        routingContext.response().putHeader("Access-Control-Allow-Origin", "*");
        routingContext.response().end(result);
    }

    protected void setSockJSHandler(Router router, Map<String, String> config, Vertx vertx) {
        SockJSHandlerOptions options = new SockJSHandlerOptions().setHeartbeatInterval(2000);
        SockJSHandler sockJSHandler = SockJSHandler.create(vertx, options);
        sockJSHandler.socketHandler(new HttpServiceHandler(config));
        String apiPath = config.get("api_path");
        router.route(apiPath).handler(sockJSHandler);
    }

    protected void setStaticHandler(Router router, Map<String, String> config) {
        String webRoot = config.get("web_root");
        for (String root : webRoot.split(",", -1)) {
            root = root.trim();
            if (root.isEmpty())
                continue;
            StaticHandler sh = StaticHandler.create(root);
            sh.setCachingEnabled(false);
            router.route("/*").handler(sh);
        }
    }
}
