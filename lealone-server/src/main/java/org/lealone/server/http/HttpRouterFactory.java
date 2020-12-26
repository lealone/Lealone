package org.lealone.server.http;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.lealone.common.util.CaseInsensitiveMap;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
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
        final HttpServiceHandler serviceHandler = new HttpServiceHandler(config);
        String servicePath = "/service/:serviceName/:methodName";
        router.post(servicePath).handler(BodyHandler.create());
        router.post(servicePath).handler(routingContext -> {
            handleHttpServiceRequest(serviceHandler, routingContext);
        });
        router.get(servicePath).handler(routingContext -> {
            handleHttpServiceRequest(serviceHandler, routingContext);
        });

        router.route().handler(CorsHandler.create("*").allowedMethod(HttpMethod.GET).allowedMethod(HttpMethod.POST));
        setSockJSHandler(router, config, vertx);
        // 放在最后
        setStaticHandler(router, config);
        return router;
    }

    protected void initRouter(Map<String, String> config, Vertx vertx, Router router) {
    }

    @SuppressWarnings("unchecked")
    protected static CaseInsensitiveMap<Object> getMethodArgs(RoutingContext routingContext) {
        CaseInsensitiveMap<Object> methodArgs = new CaseInsensitiveMap<>();
        for (Map.Entry<String, String> e : routingContext.request().params().entries()) {
            String key = e.getKey();
            String value = e.getValue();
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
        return methodArgs;
    }

    protected void handleHttpServiceRequest(final HttpServiceHandler serviceHandler, RoutingContext routingContext) {
        String serviceName = routingContext.request().params().get("serviceName");
        String methodName = routingContext.request().params().get("methodName");
        CaseInsensitiveMap<Object> methodArgs = getMethodArgs(routingContext);
        Buffer result = serviceHandler.executeService(serviceName, methodName, methodArgs);
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
