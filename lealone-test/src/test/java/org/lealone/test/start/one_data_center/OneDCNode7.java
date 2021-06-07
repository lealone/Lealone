/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.start.one_data_center;

public class OneDCNode7 extends OneDCNodeBase {
    public static void main(String[] args) {
        run(OneDCNode7.class, args);
    }

    public OneDCNode7() {
        this.listen_address = "127.0.0.7";
        this.dir = "node7";
    }
}
