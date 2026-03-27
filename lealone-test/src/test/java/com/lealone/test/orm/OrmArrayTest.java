/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.orm;

import java.sql.Array;

import org.junit.Before;
import org.junit.Test;

import com.lealone.test.orm.generated.User;

public class OrmArrayTest extends OrmTestBase {

    @Before
    @Override
    public void setUpBefore() {
        setEmbedded(true);
        setInMemory(true);
        SqlScript.createUserTable(this);
    }

    @Test
    public void run() {
        new User().id.set(1006).name.set("rob6").phones.set(new Object[] { 1, 2, 3 }).insert();
        new User().id.set(1007).name.set("中文").phones.set(new Object[] { 5, 6 }).insert();
        Array array = User.dao.findArray();
        String str = array.toString();
        System.out.println(str);
        assertTrue(!str.contains("STRINGDECODE"));
    }

}
