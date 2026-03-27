package com.lealone.test.service.generated.executor;

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
    public Value executeService(String methodName, Value[] methodArgs) {
        switch (methodName) {
        case "M1":
            List<Object> result1 = si.m1();
            if (result1 == null)
                return ValueNull.INSTANCE;
            return ValueList.get(Object.class, result1);
        case "M2":
            List<Integer> result2 = si.m2();
            if (result2 == null)
                return ValueNull.INSTANCE;
            return ValueList.get(Integer.class, result2);
        case "M3":
            Set<Object> result3 = si.m3();
            if (result3 == null)
                return ValueNull.INSTANCE;
            return ValueSet.get(Object.class, result3);
        case "M4":
            Set<String> result4 = si.m4();
            if (result4 == null)
                return ValueNull.INSTANCE;
            return ValueSet.get(String.class, result4);
        case "M5":
            Map<Object, Object> result5 = si.m5();
            if (result5 == null)
                return ValueNull.INSTANCE;
            return ValueMap.get(Object.class, Object.class, result5);
        case "M6":
            Map<Integer, String> result6 = si.m6();
            if (result6 == null)
                return ValueNull.INSTANCE;
            return ValueMap.get(Integer.class, String.class, result6);
        case "M7":
            List<Integer> p_p1_7 = methodArgs[0].getCollection();
            Set<String> p_p2_7 = methodArgs[1].getCollection();
            Map<Integer, String> p_p3_7 = methodArgs[2].getCollection();
            Integer p_p4_7 = methodArgs[3].getInt();
            Map<Integer, String> result7 = si.m7(p_p1_7, p_p2_7, p_p3_7, p_p4_7);
            if (result7 == null)
                return ValueNull.INSTANCE;
            return ValueMap.get(Integer.class, String.class, result7);
        default:
            throw noMethodException(methodName);
        }
    }

    @Override
    public Object executeService(String methodName, Map<String, Object> methodArgs) {
        switch (methodName) {
        case "M1":
            return si.m1();
        case "M2":
            return si.m2();
        case "M3":
            return si.m3();
        case "M4":
            return si.m4();
        case "M5":
            return si.m5();
        case "M6":
            return si.m6();
        case "M7":
            List<Integer> p_p1_7 = toList("P1", methodArgs);
            Set<String> p_p2_7 = toSet("P2", methodArgs);
            Map<Integer, String> p_p3_7 = toMap("P3", methodArgs);
            Integer p_p4_7 = toInt("P4", methodArgs);
            return si.m7(p_p1_7, p_p2_7, p_p3_7, p_p4_7);
        default:
            throw noMethodException(methodName);
        }
    }

    @Override
    public Object executeService(String methodName, String json) {
        JsonArray ja = null;
        switch (methodName) {
        case "M1":
            return si.m1();
        case "M2":
            return si.m2();
        case "M3":
            return si.m3();
        case "M4":
            return si.m4();
        case "M5":
            return si.m5();
        case "M6":
            return si.m6();
        case "M7":
            ja = new JsonArray(json);
            List<Integer> p_p1_7 = ja.getList(0);
            Set<String> p_p2_7 = ja.getSet(1);
            Map<Integer, String> p_p3_7 = ja.getMap(2, Integer.class);
            Integer p_p4_7 = Integer.valueOf(ja.getValue(3).toString());
            return si.m7(p_p1_7, p_p2_7, p_p3_7, p_p4_7);
        default:
            throw noMethodException(methodName);
        }
    }
}
