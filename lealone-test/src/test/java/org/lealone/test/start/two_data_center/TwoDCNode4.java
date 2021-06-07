/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.start.two_data_center;

public class TwoDCNode4 extends TwoDCNodeBase {
    public static void main(String[] args) {
        TwoDCNodeBase.run("lealone-rackdc2.properties", TwoDCNode4.class, args);
    }

    public TwoDCNode4() {
        this.listen_address = "127.0.0.4";
        this.dir = "node4";
    }
}
