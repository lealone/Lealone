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
package org.lealone.test.orm;

import org.lealone.test.UnitTestBase;
import org.lealone.test.orm.generated.User;

public class ExpressionTest extends UnitTestBase {

    public static void main(String[] args) {
        new ExpressionTest().runTest();
    }

    @Override
    public void test() {
        // 创建表: user
        execute("create table user(name char(10) primary key, notes varchar, phone int)");

        User d = User.dao;
        // select name, phone from user where name = 'zhh' and (notes = 'notes1' or notes = 'notes2')
        // group by name, phone having name = 'zhh' order by name asc, phone desc;
        d = d.select(d.name, d.phone).where().name.eq("zhh").and().lp().notes.eq("notes1").or().notes.eq("notes2").rp()
                .groupBy(d.name, d.phone).having().name.eq("zhh").orderBy().name.asc().phone.desc();

        d.printSQL();
        d.findList();
    }

}
