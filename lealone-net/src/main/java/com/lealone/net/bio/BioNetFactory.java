/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.bio;

import com.lealone.common.exceptions.DbException;
import com.lealone.net.NetClient;
import com.lealone.net.NetFactoryBase;
import com.lealone.net.NetServer;

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
