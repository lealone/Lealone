/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.vector;

import org.lealone.test.start.NodeBase;
import org.lealone.test.start.TcpServerStart;

//加以下参数才能正常运行
//--add-modules jdk.incubator.vector -server
public class VectorServerStart extends TcpServerStart {

    public static void main(String[] args) {
        NodeBase.run(TcpServerStart.class, args);
    }
}
