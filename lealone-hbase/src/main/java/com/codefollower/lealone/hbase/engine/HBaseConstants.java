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

import com.codefollower.lealone.constant.Constants;

public class HBaseConstants {
    public static final String COMMAND_RETRYABLE = Constants.PROJECT_NAME_PREFIX + "command.retryable";
    public static final boolean DEFAULT_COMMAND_RETRYABLE = true;

    public static final String SESSION_CORE_POOL_SIZE = Constants.PROJECT_NAME_PREFIX + "session.core.pool.size";
    public static final int DEFAULT_SESSION_CORE_POOL_SIZE = 3;
}
