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
package org.lealone.p2p.gms;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.p2p.net.IVersionedSerializer;

/**
 * This abstraction represents both the HeartBeatState and the ApplicationState in an NodeState
 * instance. Any state for a given node can be retrieved from this instance.
 */
public class NodeState {
    private static final ApplicationState[] STATES = ApplicationState.values();

    public final static IVersionedSerializer<NodeState> serializer = new NodeStateSerializer();

    private volatile HeartBeatState hbState;
    final Map<ApplicationState, VersionedValue> applicationState = new ConcurrentHashMap<>();

    /* fields below do not get serialized */
    private volatile long updateTimestamp;
    private volatile boolean isAlive;

    NodeState(HeartBeatState initialHbState) {
        hbState = initialHbState;
        updateTimestamp = System.nanoTime();
        isAlive = true;
    }

    HeartBeatState getHeartBeatState() {
        return hbState;
    }

    void setHeartBeatState(HeartBeatState newHbState) {
        updateTimestamp();
        hbState = newHbState;
    }

    public VersionedValue getApplicationState(ApplicationState key) {
        return applicationState.get(key);
    }

    /**
     * TODO replace this with operations that don't expose private state
     */
    // @Deprecated
    public Map<ApplicationState, VersionedValue> getApplicationStateMap() {
        return applicationState;
    }

    void addApplicationState(ApplicationState key, VersionedValue value) {
        applicationState.put(key, value);
    }

    /* getters and setters */
    /**
     * @return System.nanoTime() when state was updated last time.
     */
    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    void updateTimestamp() {
        updateTimestamp = System.nanoTime();
    }

    public boolean isAlive() {
        return isAlive;
    }

    void markAlive() {
        isAlive = true;
    }

    void markDead() {
        isAlive = false;
    }

    @Override
    public String toString() {
        return String.format("NodeState[ %s, AppStateMap = %s ]", hbState, applicationState);
    }

    private static class NodeStateSerializer implements IVersionedSerializer<NodeState> {
        @Override
        public void serialize(NodeState epState, DataOutput out, int version) throws IOException {
            /* serialize the HeartBeatState */
            HeartBeatState hbState = epState.getHeartBeatState();
            HeartBeatState.serializer.serialize(hbState, out, version);

            /* serialize the map of ApplicationState objects */
            int size = epState.applicationState.size();
            out.writeInt(size);
            for (Map.Entry<ApplicationState, VersionedValue> entry : epState.applicationState.entrySet()) {
                VersionedValue value = entry.getValue();
                out.writeInt(entry.getKey().ordinal());
                VersionedValue.serializer.serialize(value, out, version);
            }
        }

        @Override
        public NodeState deserialize(DataInput in, int version) throws IOException {
            HeartBeatState hbState = HeartBeatState.serializer.deserialize(in, version);
            NodeState epState = new NodeState(hbState);

            int appStateSize = in.readInt();
            for (int i = 0; i < appStateSize; ++i) {
                int key = in.readInt();
                VersionedValue value = VersionedValue.serializer.deserialize(in, version);
                epState.addApplicationState(NodeState.STATES[key], value);
            }
            return epState;
        }
    }
}
