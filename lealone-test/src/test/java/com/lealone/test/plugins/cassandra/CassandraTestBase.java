/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.plugins.cassandra;

import org.junit.Before;

import com.lealone.test.UnitTestBase;

public class CassandraTestBase extends UnitTestBase {

    public final static int TEST_PORT = 9610;
    public final static int CASSANDRA_PORT = 9042;

    @Before
    @Override
    public void setUpBefore() {
    }
}
