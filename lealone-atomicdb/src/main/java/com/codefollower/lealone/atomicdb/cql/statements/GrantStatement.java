/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.codefollower.lealone.atomicdb.cql.statements;

import java.util.Set;

import com.codefollower.lealone.atomicdb.auth.IResource;
import com.codefollower.lealone.atomicdb.auth.Permission;
import com.codefollower.lealone.atomicdb.config.DatabaseDescriptor;
import com.codefollower.lealone.atomicdb.exceptions.RequestExecutionException;
import com.codefollower.lealone.atomicdb.exceptions.RequestValidationException;
import com.codefollower.lealone.atomicdb.service.ClientState;
import com.codefollower.lealone.atomicdb.transport.messages.ResultMessage;

public class GrantStatement extends PermissionAlteringStatement
{
    public GrantStatement(Set<Permission> permissions, IResource resource, String username)
    {
        super(permissions, resource, username);
    }

    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        DatabaseDescriptor.getAuthorizer().grant(state.getUser(), permissions, resource, username);
        return null;
    }
}
