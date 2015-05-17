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
package org.lealone.cluster.db;

import java.util.HashMap;

import org.lealone.cluster.config.DatabaseDescriptor;
import org.lealone.cluster.exceptions.ConfigurationException;
import org.lealone.cluster.locator.AbstractReplicationStrategy;
import org.lealone.cluster.service.StorageService;
import org.lealone.dbobject.Schema;
import org.lealone.dbobject.Schema.ReplicationPropertiesChangeListener;

public class Keyspace implements ReplicationPropertiesChangeListener {
    private static final Keyspace INSTANCE = new Keyspace();
    private static final HashMap<Schema, AbstractReplicationStrategy> replicationStrategys = new HashMap<>();
    private static final AbstractReplicationStrategy defaultReplicationStrategy = DatabaseDescriptor
            .getDefaultReplicationStrategy();

    public static AbstractReplicationStrategy getReplicationStrategy(Schema schema) {
        if (schema.getReplicationProperties() == null)
            return defaultReplicationStrategy;
        AbstractReplicationStrategy replicationStrategy = replicationStrategys.get(schema);
        if (replicationStrategy == null) {
            HashMap<String, String> map = new HashMap<>(schema.getReplicationProperties());
            String className = map.remove("class");
            if (className == null) {
                throw new ConfigurationException("Missing replication strategy class");
            }

            replicationStrategy = AbstractReplicationStrategy.createReplicationStrategy(schema.getFullName(),
                    AbstractReplicationStrategy.getClass(className), StorageService.instance.getTokenMetaData(),
                    DatabaseDescriptor.getEndpointSnitch(), map);
            schema.setReplicationPropertiesChangeListener(INSTANCE);
            replicationStrategys.put(schema, replicationStrategy);
        }
        return replicationStrategy;
    }

    private Keyspace() {
    }

    @Override
    public void replicationPropertiesChanged(Schema schema) {
        replicationStrategys.remove(schema);
        getReplicationStrategy(schema);
    }
}
