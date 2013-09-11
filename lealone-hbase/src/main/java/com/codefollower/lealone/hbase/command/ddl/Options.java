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

import java.util.ArrayList;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.command.ddl.DefineCommand;
import com.codefollower.lealone.engine.Session;

public class Options extends DefineCommand {
    public static final String ON_DEFAULT_COLUMN_FAMILY_NAME = "DEFAULT_COLUMN_FAMILY_NAME";

    private ArrayList<String> optionNames;
    private ArrayList<String> optionValues;

    public Options(Session session, ArrayList<String> optionNames, ArrayList<String> optionValues) {
        super(session);
        this.optionNames = optionNames;
        this.optionValues = optionValues;
    }

    public ArrayList<String> getOptionNames() {
        return optionNames;
    }

    public void setOptionNames(ArrayList<String> optionNames) {
        this.optionNames = optionNames;
    }

    public ArrayList<String> getOptionValues() {
        return optionValues;
    }

    public void setOptionValues(ArrayList<String> optionValues) {
        this.optionValues = optionValues;
    }

    @Override
    public int getType() {
        return CommandInterface.UNKNOWN;
    }

    public void initOptions(HColumnDescriptor hcd) {
        if (optionNames != null) {
            for (int i = 0, len = optionNames.size(); i < len; i++) {
                hcd.setValue(optionNames.get(i), optionValues.get(i));
            }
        }
    }

    public void initOptions(HTableDescriptor htd) {
        if (optionNames != null) {
            for (int i = 0, len = optionNames.size(); i < len; i++) {
                htd.setValue(optionNames.get(i), optionValues.get(i));
            }
        }
    }

    public String getDefaultColumnFamilyName() {
        if (optionNames != null)
            for (int i = 0, len = optionNames.size(); i < len; i++) {
                if (ON_DEFAULT_COLUMN_FAMILY_NAME.equalsIgnoreCase(optionNames.get(i)))
                    return optionValues.get(i);
            }

        return null;
    }
}
