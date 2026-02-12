/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.service;

import java.util.List;

import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.db.plugin.Plugin;

public interface ServiceCodeGenerator extends Plugin {

    public void init(Service service, List<?> serviceMethods,
            CaseInsensitiveMap<String> serviceParameters, String codePath, boolean genCode);

    public void genServiceInterfaceCode();

    public void genServiceImplementClassCode();

    public StringBuilder genServiceExecutorCode(boolean writeFile);

}
