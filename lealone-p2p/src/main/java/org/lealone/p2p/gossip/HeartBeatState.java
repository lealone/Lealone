/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.gossip;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.lealone.p2p.gossip.protocol.IVersionedSerializer;

/**
 * HeartBeat State associated with any given node.
 */
class HeartBeatState {
    public static final IVersionedSerializer<HeartBeatState> serializer = new HeartBeatStateSerializer();

    private int generation;
    private int version;

    HeartBeatState(int gen) {
        this(gen, 0);
    }

    HeartBeatState(int gen, int ver) {
        generation = gen;
        version = ver;
    }

    int getGeneration() {
        return generation;
    }

    void updateHeartBeat() {
        version = VersionGenerator.getNextVersion();
    }

    int getHeartBeatVersion() {
        return version;
    }

    void forceNewerGenerationUnsafe() {
        generation += 1;
    }

    @Override
    public String toString() {
        return String.format("HeartBeatState[ generation = %d, version = %d ]", generation, version);
    }

    private static class HeartBeatStateSerializer implements IVersionedSerializer<HeartBeatState> {
        @Override
        public void serialize(HeartBeatState hbState, DataOutput out, int version) throws IOException {
            out.writeInt(hbState.getGeneration());
            out.writeInt(hbState.getHeartBeatVersion());
        }

        @Override
        public HeartBeatState deserialize(DataInput in, int version) throws IOException {
            return new HeartBeatState(in.readInt(), in.readInt());
        }
    }
}
