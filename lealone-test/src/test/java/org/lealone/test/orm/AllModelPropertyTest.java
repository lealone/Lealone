/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.orm;

import org.junit.Test;
import org.lealone.test.UnitTestBase;

public class AllModelPropertyTest extends UnitTestBase {

    @Test
    public void run() {
        SqlScript.createAllModelPropertyTable(this);
    }
}
