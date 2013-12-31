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
package com.codefollower.lealone.atomicdb.cql.statements;

import java.util.*;

import com.codefollower.lealone.atomicdb.auth.*;
import com.codefollower.lealone.atomicdb.config.DatabaseDescriptor;
import com.codefollower.lealone.atomicdb.cql.ColumnIdentifier;
import com.codefollower.lealone.atomicdb.cql.ColumnSpecification;
import com.codefollower.lealone.atomicdb.cql.ResultSet;
import com.codefollower.lealone.atomicdb.db.marshal.UTF8Type;
import com.codefollower.lealone.atomicdb.exceptions.InvalidRequestException;
import com.codefollower.lealone.atomicdb.exceptions.RequestExecutionException;
import com.codefollower.lealone.atomicdb.exceptions.RequestValidationException;
import com.codefollower.lealone.atomicdb.service.ClientState;
import com.codefollower.lealone.atomicdb.transport.messages.ResultMessage;

public class ListPermissionsStatement extends AuthorizationStatement
{
    private static final String KS = Auth.AUTH_KS;
    private static final String CF = "permissions"; // virtual cf to use for now.

    private static final List<ColumnSpecification> metadata;

    static
    {
        List<ColumnSpecification> columns = new ArrayList<ColumnSpecification>(4);
        columns.add(new ColumnSpecification(KS, CF, new ColumnIdentifier("username", true), UTF8Type.instance));
        columns.add(new ColumnSpecification(KS, CF, new ColumnIdentifier("resource", true), UTF8Type.instance));
        columns.add(new ColumnSpecification(KS, CF, new ColumnIdentifier("permission", true), UTF8Type.instance));
        metadata = Collections.unmodifiableList(columns);
    }

    private final Set<Permission> permissions;
    private DataResource resource;
    private final String username;
    private final boolean recursive;

    public ListPermissionsStatement(Set<Permission> permissions, IResource resource, String username, boolean recursive)
    {
        this.permissions = permissions;
        this.resource = (DataResource) resource;
        this.username = username;
        this.recursive = recursive;
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        // a check to ensure the existence of the user isn't being leaked by user existence check.
        state.ensureNotAnonymous();

        if (username != null && !Auth.isExistingUser(username))
            throw new InvalidRequestException(String.format("User %s doesn't exist", username));

        if (resource != null)
        {
            resource = maybeCorrectResource(resource, state);
            if (!resource.exists())
                throw new InvalidRequestException(String.format("%s doesn't exist", resource));
        }
    }

    public void checkAccess(ClientState state)
    {
        // checked in validate
    }

    // TODO: Create a new ResultMessage type (?). Rows will do for now.
    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        List<PermissionDetails> details = new ArrayList<PermissionDetails>();

        if (resource != null && recursive)
        {
            for (IResource r : Resources.chain(resource))
                details.addAll(list(state, r));
        }
        else
        {
            details.addAll(list(state, resource));
        }

        Collections.sort(details);
        return resultMessage(details);
    }

    private ResultMessage resultMessage(List<PermissionDetails> details)
    {
        if (details.isEmpty())
            return new ResultMessage.Void();

        ResultSet result = new ResultSet(metadata);
        for (PermissionDetails pd : details)
        {
            result.addColumnValue(UTF8Type.instance.decompose(pd.username));
            result.addColumnValue(UTF8Type.instance.decompose(pd.resource.toString()));
            result.addColumnValue(UTF8Type.instance.decompose(pd.permission.toString()));
        }
        return new ResultMessage.Rows(result);
    }

    private Set<PermissionDetails> list(ClientState state, IResource resource)
    throws RequestValidationException, RequestExecutionException
    {
        return DatabaseDescriptor.getAuthorizer().list(state.getUser(), permissions, resource, username);
    }
}
