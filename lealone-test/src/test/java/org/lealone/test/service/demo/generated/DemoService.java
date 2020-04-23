package org.lealone.test.service.demo.generated;

import org.lealone.client.ClientServiceProxy;
import org.lealone.orm.json.JsonArray;

/**
 * Service interface for 'demo_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public interface DemoService {

    static DemoService create(String url) {
        return new Proxy(url);
    }

    String sayHello(String name);

    static class Proxy implements DemoService {

        private final String url;

        private Proxy(String url) {
            this.url = url;
        }

        @Override
        public String sayHello(String name) {
            JsonArray ja = new JsonArray();
            ja.add(name);
            String result = ClientServiceProxy.executeWithReturnValue(url, "DEMO_SERVICE.SAY_HELLO", ja.encode());
            if (result != null) {
                return result;
            }
            return null;
        }
    }
}
