/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.start.two_data_center;

public class TwoDCNode1 extends TwoDCNodeBase {
    public static void main(String[] args) {
        TwoDCNodeBase.run("lealone-rackdc1.properties", TwoDCNode1.class, args);
    }

    public TwoDCNode1() {
        this.listen_address = "127.0.0.1";
        this.dir = "node1";
    }
}
