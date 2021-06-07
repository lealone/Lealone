/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.start.two_data_center;

public class TwoDCNode2 extends TwoDCNodeBase {
    public static void main(String[] args) {
        TwoDCNodeBase.run("lealone-rackdc1-2.properties", TwoDCNode2.class, args);
    }

    public TwoDCNode2() {
        this.listen_address = "127.0.0.2";
        this.dir = "node2";
    }
}
