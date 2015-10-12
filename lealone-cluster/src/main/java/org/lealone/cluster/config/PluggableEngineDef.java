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
package org.lealone.cluster.config;

import java.util.HashMap;
import java.util.Map;

import org.lealone.cluster.exceptions.ConfigurationException;

public class PluggableEngineDef {
    public String name;
    public boolean enabled;

    // 不能用Map，就算用TypeDescription.putMapPropertyType注册了，
    // 在Config类中使用List<PluggableEngineDef>时还是会出错
    public Object parameters;

    private Map<String, String> map;

    public PluggableEngineDef() {
    }

    public Map<String, String> getParameters() {
        if (map != null)
            return map;
        map = new HashMap<>();

        if (parameters != null) {
            if (!(parameters instanceof Map<?, ?>)) {
                throw new ConfigurationException("Engine parameters must be a map");
            }
            Map<?, ?> parameters = (Map<?, ?>) this.parameters;
            for (Object k : parameters.keySet()) {
                Object v = parameters.get(k);
                if (v == null)
                    throw new ConfigurationException("Engine parameter value is null, key=" + k);
                map.put(k.toString(), v.toString());
            }
        }
        return map;
    }
}
