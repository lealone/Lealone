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
package org.lealone.test.service.impl;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.UUID;

import org.lealone.test.orm.generated.User;
import org.lealone.test.service.generated.AllTypeService;

public class AllTypeServiceImpl implements AllTypeService {

    @Override
    public User testType(Integer f1, Boolean f2, Byte f3, Short f4, Long f5, Long f6, BigDecimal f7, Double f8,
            Float f9, Time f10, Date f11, Timestamp f12, byte[] f13, Object f14, String f15, String f16, String f17,
            Blob f18, Clob f19, UUID f20, Array f21) {
        return null;
    }

}
