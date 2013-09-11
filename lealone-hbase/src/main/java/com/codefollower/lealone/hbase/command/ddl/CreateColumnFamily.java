/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package com.codefollower.lealone.hbase.command.ddl;

import org.apache.hadoop.hbase.HColumnDescriptor;

import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.command.ddl.DefineCommand;
import com.codefollower.lealone.engine.Session;

public class CreateColumnFamily extends DefineCommand {

    private final String cfName;
    private final Options options;

    public CreateColumnFamily(Session session, String cfName, Options options) {
        super(session);
        this.cfName = cfName;
        this.options = options;
    }

    public String getColumnFamilyName() {
        return cfName;
    }

    public int update() {
        return 0;
    }

    public int getType() {
        return CommandInterface.UNKNOWN;
    }

    public HColumnDescriptor createHColumnDescriptor(Options defaultColumnFamilyOptions) {
        HColumnDescriptor hcd = new HColumnDescriptor(cfName);
        if (options != null) {
            options.initOptions(hcd);
        } else if (defaultColumnFamilyOptions != null) {
            defaultColumnFamilyOptions.initOptions(hcd);
        }
        return hcd;
    }

}
