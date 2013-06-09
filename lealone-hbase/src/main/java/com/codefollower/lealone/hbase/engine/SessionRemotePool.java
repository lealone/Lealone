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
package com.codefollower.lealone.hbase.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.engine.ConnectionInfo;
import com.codefollower.lealone.engine.SessionInterface;
import com.codefollower.lealone.engine.SessionRemote;
import com.codefollower.lealone.expression.Parameter;
import com.codefollower.lealone.expression.ParameterInterface;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.util.New;

public class SessionRemotePool {
    private static final Map<String, SessionRemote> sessionRemoteCache = New.hashMap();
    private static final Map<String, SessionRemote> sessionRemoteCacheMaybeWithMaster = New.hashMap();

    private static SessionRemote masterSessionRemote;

    public static void addSessionRemote(String url, SessionRemote sessionRemote, boolean isMaster) {
        sessionRemoteCacheMaybeWithMaster.put(url, sessionRemote);
        if (!isMaster)
            sessionRemoteCache.put(url, sessionRemote);
    }

    public static SessionRemote getSessionRemote(String url) {
        return sessionRemoteCacheMaybeWithMaster.get(url);
    }

    public static SessionRemote getMasterSessionRemote(Properties info) {
        if (masterSessionRemote == null) {
            synchronized (SessionRemotePool.class) {
                if (masterSessionRemote == null) {
                    try {
                        masterSessionRemote = (SessionRemote) getSessionInterface(info, HBaseUtils.getMasterURL());
                    } catch (IOException e) {
                        throw DbException.convert(e);
                    }
                }
            }
        }
        return masterSessionRemote;
    }

    public static CommandInterface getMasterCommand(Properties info, String sql, List<Parameter> parameters) {
        return getCommandInterface(getMasterSessionRemote(info), sql, parameters);
    }

    public static SessionInterface getSessionInterface(Properties info, String url) {
        Properties prop = new Properties();
        for (String key : info.stringPropertyNames())
            prop.setProperty(key, info.getProperty(key));
        ConnectionInfo ci = new ConnectionInfo(url, prop);

        return new SessionRemote(ci).connectEmbeddedOrServer(false);
    }

    public static CommandInterface getCommandInterface(SessionRemote sessionRemote, String sql, List<Parameter> parameters) {
        CommandInterface commandInterface = sessionRemote.prepareCommand(sql, -1); //此时fetchSize还未知

        //传递最初的参数值到新的CommandInterface
        if (parameters != null && !parameters.isEmpty()) {
            ArrayList<? extends ParameterInterface> newParams = commandInterface.getParameters();
            for (int i = 0, size = parameters.size(); i < size; i++) {
                newParams.get(i).setValue(parameters.get(i).getParamValue(), true);
            }
        }

        return commandInterface;
    }
}
