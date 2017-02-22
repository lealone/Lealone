/*
 * Copyright (c) 2009 Red Hat, Inc.
 * -------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package org.lealone.common.logging;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.LogManager;

import org.lealone.common.logging.spi.LogDelegate;
import org.lealone.common.logging.spi.LogDelegateFactory;

/**
 * A {@link org.lealone.common.logging.spi.LogDelegateFactory} which creates {@link JULLogDelegate} instances.
 *
 * @author <a href="kenny.macleod@kizoom.com">Kenny MacLeod</a>
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class JULLogDelegateFactory implements LogDelegateFactory {

    static {
        // Try and load Lealone JUL default logging config from classpath
        if (System.getProperty("java.util.logging.config.file") == null) {
            try (InputStream is = JULLogDelegateFactory.class.getClassLoader()
                    .getResourceAsStream("lealone-default-jul-logging.properties")) {
                if (is != null) {
                    LogManager.getLogManager().readConfiguration(is);
                }
            } catch (IOException ignore) {
            }
        }
    }

    @Override
    public LogDelegate createDelegate(final String name) {
        return new JULLogDelegate(name);
    }
}
