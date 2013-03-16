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
package com.codefollower.lealone.omid;

import java.io.File;

import com.codefollower.lealone.omid.tso.TSOServer;

/**
 * 
 * 如果出现java.lang.UnsatisfiedLinkError: no tso-commithashmap in java.library.path
 * 
 * 请加上VM参数: -Djava.library.path=<tso-commithashmap path> (见下面的输出信息"tso-commithashmap path: ")
 * 
 * 如: -Djava.library.path=E:\lealone\git\lealone-omid\src\test\resources
 *
 */
public class TSOServerStarter {
    public static void main(String[] args) throws Exception {
        System.out.println("tso-commithashmap path: " + new File("src/test/resources").getCanonicalPath());
        //这种方式无效
        //System.setProperty("java.library.path", new File("src/test/resources").getCanonicalPath() + File.pathSeparatorChar
        //        + System.getProperty("java.library.path"));

        args = new String[] { "-zk", "127.0.0.1:2181", "-port", "1234", "-ha", "-ensemble", "1", "-quorum", "1" };
        TSOServer.main(args);
    }
}
