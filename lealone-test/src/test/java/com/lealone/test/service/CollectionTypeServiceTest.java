/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.service;

import org.junit.Test;

import com.lealone.db.LealoneDatabase;
import com.lealone.test.orm.SqlScript;
import com.lealone.test.sql.SqlTestBase;

public class CollectionTypeServiceTest extends SqlTestBase {

    public CollectionTypeServiceTest() {
        super(LealoneDatabase.NAME);
    }

    @Test
    public void testService() throws Exception {
        SqlScript.createCollectionTypeService(this);
    }
}
