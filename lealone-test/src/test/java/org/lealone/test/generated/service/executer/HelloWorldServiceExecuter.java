package org.lealone.test.generated.service.executer;

import io.vertx.core.json.JsonArray;
import org.lealone.db.service.ServiceExecuter;
import org.lealone.test.service.impl.HelloWorldServiceImpl;

/**
 * Service executer for 'hello_world_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class HelloWorldServiceExecuter implements ServiceExecuter {

    private final HelloWorldServiceImpl s = new HelloWorldServiceImpl();

    public HelloWorldServiceExecuter() {
    }

    @Override
    public String executeService(String methodName, String json) {
        JsonArray ja = null;
        switch (methodName) {
        case "SAY_HELLO":
            this.s.sayHello();
            break;
        case "SAY_GOODBYE_TO":
            ja = new JsonArray(json);
            String p_name2 = ja.getString(0);
            String result2 = this.s.sayGoodbyeTo(p_name2);
            if (result2 == null)
                return null;
            return result2;
        default:
            throw new RuntimeException("no method: " + methodName);
        }
        return NO_RETURN_VALUE;
    }
}
