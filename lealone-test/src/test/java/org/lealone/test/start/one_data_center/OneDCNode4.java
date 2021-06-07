/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.start.one_data_center;

public class OneDCNode4 extends OneDCNodeBase {
    public static void main(String[] args) {
        run(OneDCNode4.class, args);
    }

    public OneDCNode4() {
        this.listen_address = "127.0.0.4";
        this.dir = "node4";
    }
}
