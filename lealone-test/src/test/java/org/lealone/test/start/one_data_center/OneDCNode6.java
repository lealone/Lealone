/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.start.one_data_center;

public class OneDCNode6 extends OneDCNodeBase {
    public static void main(String[] args) {
        run(OneDCNode6.class, args);
    }

    public OneDCNode6() {
        this.listen_address = "127.0.0.6";
        this.dir = "node6";
    }
}
