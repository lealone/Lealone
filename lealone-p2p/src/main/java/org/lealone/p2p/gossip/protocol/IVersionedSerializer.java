/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.gossip.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface IVersionedSerializer<T> {
    /**
     * Serialize the specified type into the specified DataOutputStream instance.
     *
     * @param t type that needs to be serialized
     * @param out DataOutput into which serialization needs to happen.
     * @param version protocol version
     * @throws java.io.IOException if serialization fails
     */
    public void serialize(T t, DataOutput out, int version) throws IOException;

    /**
     * Deserialize into the specified DataInputStream instance.
     * @param in DataInput from which deserialization needs to happen.
     * @param version protocol version
     * @return the type that was deserialized
     * @throws IOException if deserialization fails
     */
    public T deserialize(DataInput in, int version) throws IOException;
}
