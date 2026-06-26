/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lealone.server.http.container.valves;

import com.lealone.common.logging.Logger;
import com.lealone.server.http.container.Contained;
import com.lealone.server.http.container.Container;
import com.lealone.server.http.container.LifecycleException;
import com.lealone.server.http.container.LifecycleState;
import com.lealone.server.http.container.Valve;
import com.lealone.server.http.container.util.LifecycleMBeanBase;
import com.lealone.server.http.container.util.ToStringUtil;
import com.lealone.server.http.util.res.StringManager;

/**
 * Convenience base class for implementations of the <b>Valve</b> interface. A subclass <strong>MUST</strong> implement
 * an <code>invoke()</code> method to provide the required functionality, and <strong>MAY</strong> implement the
 * <code>Lifecycle</code> interface to provide configuration management and lifecycle support.
 */
public abstract class ValveBase extends LifecycleMBeanBase implements Contained, Valve {

    protected static final StringManager sm = StringManager.getManager(ValveBase.class);

    // ------------------------------------------------------ Constructor

    public ValveBase() {
        this(false);
    }

    public ValveBase(boolean asyncSupported) {
        this.asyncSupported = asyncSupported;
    }

    // ------------------------------------------------------ Instance Variables

    /**
     * Does this valve support Servlet 3+ async requests?
     */
    protected boolean asyncSupported;

    /**
     * The Container whose pipeline this Valve is a component of.
     */
    protected Container container = null;

    /**
     * Container log
     */
    protected Logger containerLog = null;

    /**
     * The next Valve in the pipeline this Valve is a component of.
     */
    protected Valve next = null;

    // -------------------------------------------------------------- Properties

    @Override
    public Container getContainer() {
        return container;
    }

    @Override
    public void setContainer(Container container) {
        this.container = container;
    }

    @Override
    public boolean isAsyncSupported() {
        return asyncSupported;
    }

    public void setAsyncSupported(boolean asyncSupported) {
        this.asyncSupported = asyncSupported;
    }

    @Override
    public Valve getNext() {
        return next;
    }

    @Override
    public void setNext(Valve valve) {
        this.next = valve;
    }

    // ---------------------------------------------------------- Public Methods

    /**
     * {@inheritDoc}
     * <p>
     * The default implementation is NO-OP.
     */
    @Override
    public void backgroundProcess() {
        // NOOP by default
    }

    @Override
    protected void initInternal() throws LifecycleException {
        super.initInternal();
        containerLog = getContainer().getLogger();
    }

    /**
     * Start this component and implement the requirements of
     * {@link com.lealone.server.http.container.util.LifecycleBase#startInternal()}.
     *
     * @exception LifecycleException if this component detects a fatal error that prevents this component from being
     *                                   used
     */
    @Override
    protected void startInternal() throws LifecycleException {
        setState(LifecycleState.STARTING);
    }

    /**
     * Stop this component and implement the requirements of
     * {@link com.lealone.server.http.container.util.LifecycleBase#stopInternal()}.
     *
     * @exception LifecycleException if this component detects a fatal error that prevents this component from being
     *                                   used
     */
    @Override
    protected void stopInternal() throws LifecycleException {
        setState(LifecycleState.STOPPING);
    }

    @Override
    public String toString() {
        return ToStringUtil.toString(this);
    }
}
