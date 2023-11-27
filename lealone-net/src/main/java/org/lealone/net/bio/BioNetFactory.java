/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net.bio;

import org.lealone.common.exceptions.DbException;
import org.lealone.net.NetClient;
import org.lealone.net.NetFactoryBase;
import org.lealone.net.NetServer;

public class BioNetFactory extends NetFactoryBase {

    public static final String NAME = "bio";
    public static final BioNetFactory INSTANCE = new BioNetFactory();

    public BioNetFactory() {
        super(NAME);
    }

    @Override
    public NetClient createNetClient() {
        return new BioNetClient();
    }

    @Override
    public NetServer createNetServer() {
        throw DbException.getInternalError();
    }

    @Override
    public boolean isBio() {
        return true;
    }
}
