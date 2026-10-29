package com.lealone.test.service.generated.executor;

import com.lealone.db.async.Future;
import com.lealone.db.service.ServiceExecutor;
import com.lealone.db.value.*;
import com.lealone.orm.json.JsonArray;
import com.lealone.test.service.impl.CollectionTypeServiceImpl;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Service executor for 'collection_type_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class CollectionTypeServiceExecutor implements ServiceExecutor {

    private final CollectionTypeServiceImpl si = new CollectionTypeServiceImpl();

    @Override
    public Future<?> executeServiceAsync(String methodName, Value[] methodArgs) {
        switch (methodName) {
        case "M1":
            return si.m1Async();
        case "M2":
            return si.m2Async();
        case "M3":
            return si.m3Async();
        case "M4":
            return si.m4Async();
        case "M5":
            return si.m5Async();
        case "M6":
            return si.m6Async();
        case "M7":
            List<Integer> p_p1_7 = methodArgs[0].getCollection();
            Set<String> p_p2_7 = methodArgs[1].getCollection();
            Map<Integer, String> p_p3_7 = methodArgs[2].getCollection();
            Integer p_p4_7 = methodArgs[3].getInt();
            return si.m7Async(p_p1_7, p_p2_7, p_p3_7, p_p4_7);
        default:
            return Future.failedFuture(noMethodException(methodName));
        }
    }

    @Override
    public Future<?> executeServiceAsync(String methodName, Map<String, Object> methodArgs) {
        switch (methodName) {
        case "M1":
            return si.m1Async();
        case "M2":
            return si.m2Async();
        case "M3":
            return si.m3Async();
        case "M4":
            return si.m4Async();
        case "M5":
            return si.m5Async();
        case "M6":
            return si.m6Async();
        case "M7":
            List<Integer> p_p1_7 = toList("P1", methodArgs);
            Set<String> p_p2_7 = toSet("P2", methodArgs);
            Map<Integer, String> p_p3_7 = toMap("P3", methodArgs);
            Integer p_p4_7 = toInt("P4", methodArgs);
            return si.m7Async(p_p1_7, p_p2_7, p_p3_7, p_p4_7);
        default:
            return Future.failedFuture(noMethodException(methodName));
        }
    }

    @Override
    public Future<?> executeServiceAsync(String methodName, String json) {
        JsonArray ja = null;
        switch (methodName) {
        case "M1":
            return si.m1Async();
        case "M2":
            return si.m2Async();
        case "M3":
            return si.m3Async();
        case "M4":
            return si.m4Async();
        case "M5":
            return si.m5Async();
        case "M6":
            return si.m6Async();
        case "M7":
            ja = new JsonArray(json);
            List<Integer> p_p1_7 = ja.getList(0);
            Set<String> p_p2_7 = ja.getSet(1);
            Map<Integer, String> p_p3_7 = ja.getMap(2, Integer.class);
            Integer p_p4_7 = Integer.valueOf(ja.getValue(3).toString());
            return si.m7Async(p_p1_7, p_p2_7, p_p3_7, p_p4_7);
        default:
            return Future.failedFuture(noMethodException(methodName));
        }
    }
}
