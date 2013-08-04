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
import com.codefollower.lealone.server.pg.PgServer;

public class HBaseConstants {
    public static final String HBASE_DB_NAME = "hbasedb";
    public static final String DEFAULT_TABLE_ENGINE = Constants.PROJECT_NAME_PREFIX + "default.table.engine";
    public static final String DEFAULT_DATABASE_ENGINE = Constants.PROJECT_NAME_PREFIX + "default.database.engine";

    //tcp server相关参数
    //-------------------------------
    public static final String MASTER_TCP_PORT = Constants.PROJECT_NAME_PREFIX + "master.tcp.port";
    public static final int DEFAULT_MASTER_TCP_PORT = Constants.DEFAULT_TCP_PORT - 1;

    public static final String REGIONSERVER_TCP_PORT = Constants.PROJECT_NAME_PREFIX + "regionserver.tcp.port";
    public static final int DEFAULT_REGIONSERVER_TCP_PORT = Constants.DEFAULT_TCP_PORT;

    public static final String TCP_SERVER_START_ARGS = Constants.PROJECT_NAME_PREFIX + "tcp.server.start.args";
    public static final String[] DEFAULT_TCP_SERVER_START_ARGS = { "-tcpAllowOthers", "-tcpDaemon" };

    //pg server相关参数
    //-------------------------------
    public static final String MASTER_PG_PORT = Constants.PROJECT_NAME_PREFIX + "master.pg.port";
    public static final int DEFAULT_MASTER_PG_PORT = PgServer.DEFAULT_PORT - 1;

    public static final String REGIONSERVER_PG_PORT = Constants.PROJECT_NAME_PREFIX + "regionserver.pg.port";
    public static final int DEFAULT_REGIONSERVER_PG_PORT = PgServer.DEFAULT_PORT;

    public static final String PG_SERVER_START_ARGS = Constants.PROJECT_NAME_PREFIX + "pg.server.start.args";
    public static final String[] DEFAULT_PG_SERVER_START_ARGS = { "-pgAllowOthers", "-pgDaemon" };

    public static final String PG_SERVER_ENABLED = Constants.PROJECT_NAME_PREFIX + "pg.server.enabled";
    public static final boolean DEFAULT_PG_SERVER_ENABLED = false;

    //command相关参数
    //-------------------------------
    public static final String COMMAND_RETRYABLE = Constants.PROJECT_NAME_PREFIX + "command.retryable";
    public static final boolean DEFAULT_COMMAND_RETRYABLE = true;

    public static final String COMMAND_PARALLEL_CORE_POOL_SIZE = Constants.PROJECT_NAME_PREFIX
            + "command.parallel.core.pool.size";
    public static final int DEFAULT_COMMAND_PARALLEL_CORE_POOL_SIZE = 3;

    public static final String COMMAND_PARALLEL_MAX_POOL_SIZE = Constants.PROJECT_NAME_PREFIX //
            + "command.parallel.max.pool.size";
    public static final int DEFAULT_COMMAND_PARALLEL_MAX_POOL_SIZE = Integer.MAX_VALUE;

    public static final String COMMAND_PARALLEL_KEEP_ALIVE_TIME = Constants.PROJECT_NAME_PREFIX
            + "command.parallel.keep.alive.time";
    public static final int DEFAULT_COMMAND_PARALLEL_KEEP_ALIVE_TIME = 5;

    //session相关参数
    //-------------------------------
    public static final String SESSION_CORE_POOL_SIZE = Constants.PROJECT_NAME_PREFIX + "session.core.pool.size";
    public static final int DEFAULT_SESSION_CORE_POOL_SIZE = 3;

    //metadata相关参数
    //-------------------------------
    public static final String METADATA_MAX_DDL_REDO_RECORDS = Constants.PROJECT_NAME_PREFIX + "metadata.max.ddl.redo.records";
    public static final int DEFAULT_METADATA_MAX_DDL_REDO_RECORDS = 5000;

    //transaction相关参数
    //-------------------------------
    public static final String TRANSACTION_COMMIT_CACHE_SIZE = Constants.PROJECT_NAME_PREFIX + "transaction.commit.cache.size";
    public static final int DEFAULT_TRANSACTION_COMMIT_CACHE_SIZE = 1000;

    public static final String TRANSACTION_COMMIT_CACHE_ASSOCIATIVITY = Constants.PROJECT_NAME_PREFIX
            + "transaction.commit.cache.associativity";
    public static final int DEFAULT_TRANSACTION_COMMIT_CACHE_ASSOCIATIVITY = 32;

    public static final String TRANSACTION_TIMESTAMP_BATCH = Constants.PROJECT_NAME_PREFIX + "transaction.timestamp.batch";
    public static final int DEFAULT_TRANSACTION_TIMESTAMP_BATCH = 100000;

    public static final String TRANSACTION_STATUS_CACHE_BUCKET_NUMBER = Constants.PROJECT_NAME_PREFIX
            + "transaction.status.cache.bucket.number";
    public static final int DEFAULT_TRANSACTION_STATUS_CACHE_BUCKET_NUMBER = 1 << 15;

    public static final String TRANSACTION_STATUS_CACHE_BUCKET_SIZE = Constants.PROJECT_NAME_PREFIX
            + "transaction.status.cache.bucket.size";
    public static final int DEFAULT_TRANSACTION_STATUS_CACHE_BUCKET_SIZE = 1 << 14;
}
