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
package com.codefollower.lealone.atomicdb.cql;

import java.nio.ByteBuffer;

import com.codefollower.lealone.atomicdb.config.*;
import com.codefollower.lealone.atomicdb.db.*;
import com.codefollower.lealone.atomicdb.serializers.MarshalException;
import com.codefollower.lealone.atomicdb.utils.FBUtilities;

/**
 * This has a lot of building blocks for CassandraServer to call to make sure it has valid input
 * -- ensuring column names conform to the declared comparator, for instance.
 *
 * The methods here mostly try to do just one part of the validation so they can be combined
 * for different needs -- supercolumns vs regular, range slices vs named, batch vs single-column.
 * (ValidateColumnPath is the main exception in that it includes keyspace and CF validation.)
 */
public class OldThriftValidation
{
    public static void validateKey(CFMetaData metadata, ByteBuffer key) throws com.codefollower.lealone.atomicdb.exceptions.InvalidRequestException
    {
        if (key == null || key.remaining() == 0)
        {
            throw new com.codefollower.lealone.atomicdb.exceptions.InvalidRequestException("Key may not be empty");
        }

        // check that key can be handled by FBUtilities.writeShortByteArray
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            throw new com.codefollower.lealone.atomicdb.exceptions.InvalidRequestException("Key length of " + key.remaining() +
                                                                              " is longer than maximum of " +
                                                                              FBUtilities.MAX_UNSIGNED_SHORT);
        }

        try
        {
            metadata.getKeyValidator().validate(key);
        }
        catch (MarshalException e)
        {
            throw new com.codefollower.lealone.atomicdb.exceptions.InvalidRequestException(e.getMessage());
        }
    }

    public static void validateKeyspace(String keyspaceName) throws KeyspaceNotDefinedException
    {
        if (!Schema.instance.getKeyspaces().contains(keyspaceName))
        {
            throw new KeyspaceNotDefinedException("Keyspace " + keyspaceName + " does not exist");
        }
    }

    public static CFMetaData validateColumnFamily(String keyspaceName, String cfName, boolean isCommutativeOp) throws com.codefollower.lealone.atomicdb.exceptions.InvalidRequestException
    {
        CFMetaData metadata = validateColumnFamily(keyspaceName, cfName);

        if (isCommutativeOp)
        {
            if (!metadata.getDefaultValidator().isCommutative())
                throw new com.codefollower.lealone.atomicdb.exceptions.InvalidRequestException("invalid operation for non commutative columnfamily " + cfName);
        }
        else
        {
            if (metadata.getDefaultValidator().isCommutative())
                throw new com.codefollower.lealone.atomicdb.exceptions.InvalidRequestException("invalid operation for commutative columnfamily " + cfName);
        }
        return metadata;
    }

    // To be used when the operation should be authorized whether this is a counter CF or not
    public static CFMetaData validateColumnFamily(String keyspaceName, String cfName) throws com.codefollower.lealone.atomicdb.exceptions.InvalidRequestException
    {
        validateKeyspace(keyspaceName);
        if (cfName.isEmpty())
            throw new com.codefollower.lealone.atomicdb.exceptions.InvalidRequestException("non-empty columnfamily is required");

        CFMetaData metadata = Schema.instance.getCFMetaData(keyspaceName, cfName);
        if (metadata == null)
            throw new com.codefollower.lealone.atomicdb.exceptions.InvalidRequestException("unconfigured columnfamily " + cfName);

        return metadata;
    }

    public static void validateKeyspaceNotSystem(String modifiedKeyspace) throws com.codefollower.lealone.atomicdb.exceptions.InvalidRequestException
    {
        if (modifiedKeyspace.equalsIgnoreCase(Keyspace.SYSTEM_KS))
            throw new com.codefollower.lealone.atomicdb.exceptions.InvalidRequestException("system keyspace is not user-modifiable");
    }
}
