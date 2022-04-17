/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.orm;

import org.junit.Test;

public class AllModelPropertyTest extends OrmTestBase {
    @Test
    public void run() {
        SqlScript.createAllModelPropertyTable(this);
    }
}
