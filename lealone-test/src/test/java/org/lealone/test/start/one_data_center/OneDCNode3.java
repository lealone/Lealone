/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.start.one_data_center;

public class OneDCNode3 extends OneDCNodeBase {
    public static void main(String[] args) {
        run(OneDCNode3.class, args);
    }

    public OneDCNode3() {
        this.listen_address = "127.0.0.3";
        this.dir = "node3";
    }
}
