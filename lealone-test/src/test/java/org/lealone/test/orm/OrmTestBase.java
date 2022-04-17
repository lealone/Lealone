/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.orm;

import org.junit.Before;
import org.lealone.test.UnitTestBase;

public abstract class OrmTestBase extends UnitTestBase {
    @Before
    @Override
    public void setUpBefore() {
        setEmbedded(true);
        setInMemory(true);
        // String sql = "select count(*) from information_schema.tables where table_name='CUSTOMER'";
        // if (count(sql) <= 0) {
        SqlScript.createTables(this);
        // }
    }
}
