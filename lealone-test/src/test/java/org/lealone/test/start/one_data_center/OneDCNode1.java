/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.start.one_data_center;

public class OneDCNode1 extends OneDCNodeBase {
    public static void main(String[] args) {
        run(OneDCNode1.class, args);
    }

    public OneDCNode1() {
        this.listen_address = "127.0.0.1";
        this.dir = "node1";
    }
}
