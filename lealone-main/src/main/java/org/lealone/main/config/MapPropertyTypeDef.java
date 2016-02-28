/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.main.config;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.lealone.common.exceptions.ConfigurationException;

public abstract class MapPropertyTypeDef {
    public String class_name;
    public Map<String, String> parameters;

    public MapPropertyTypeDef() {
    }

    public MapPropertyTypeDef(Map<String, ?> p) {
        class_name = (String) p.get("class_name");
        List<?> list = (List<?>) p.get("parameters");
        Map<?, ?> map;
        Object v;
        parameters = new LinkedHashMap<>();
        for (int i = 0; i < list.size(); i++) {
            map = (Map<?, ?>) list.get(i);
            if (map != null) {
                for (Object k : map.keySet()) {
                    v = map.get(k);
                    if (v == null)
                        throw new ConfigurationException("value is null, key=" + k);
                    parameters.put(k.toString(), v.toString());
                }
            }
        }
        // 这种方式有bug，取到的值有可能不是String类型
        // parameters = (Map<String, String>) ((List<?>) p.get("parameters")).get(0);
    }
}
