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
package com.lealone.server.http.container.core;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.server.http.container.Container;
import com.lealone.server.http.container.Engine;
import com.lealone.server.http.container.LifecycleException;
import com.lealone.server.http.container.LifecycleState;
import com.lealone.server.http.container.Server;
import com.lealone.server.http.container.Service;
import com.lealone.server.http.container.connector.Connector;
import com.lealone.server.http.container.mapper.Mapper;
import com.lealone.server.http.container.mapper.MapperListener;
import com.lealone.server.http.container.util.LifecycleMBeanBase;
import com.lealone.server.http.util.res.StringManager;

/**
 * Standard implementation of the <code>Service</code> interface. The associated Container is generally an instance of
 * Engine, but this is not required.
 */

public class StandardService extends LifecycleMBeanBase implements Service {

    private static final Logger log = LoggerFactory.getLogger(StandardService.class);
    private static final StringManager sm = StringManager.getManager(StandardService.class);

    // ----------------------------------------------------- Instance Variables

    /**
     * The name of this service.
     */
    private String name = null;

    /**
     * The <code>Server</code> that owns this Service, if any.
     */
    private Server server = null;

    /**
     * The property change support for this component.
     */
    protected final PropertyChangeSupport support = new PropertyChangeSupport(this);

    /**
     * The set of Connectors associated with this Service.
     */
    protected Connector[] connectors = new Connector[0];
    private final ReadWriteLock connectorsLock = new ReentrantReadWriteLock();

    private Engine engine = null;

    /**
     * Mapper.
     */
    protected final Mapper mapper = new Mapper();

    /**
     * Mapper listener.
     */
    protected final MapperListener mapperListener = new MapperListener(this);

    private long gracefulStopAwaitMillis = 0;

    // ------------------------------------------------------------- Properties

    public long getGracefulStopAwaitMillis() {
        return gracefulStopAwaitMillis;
    }

    public void setGracefulStopAwaitMillis(long gracefulStopAwaitMillis) {
        this.gracefulStopAwaitMillis = gracefulStopAwaitMillis;
    }

    @Override
    public Mapper getMapper() {
        return mapper;
    }

    @Override
    public Engine getContainer() {
        return engine;
    }

    @Override
    public void setContainer(Engine engine) {
        Engine oldEngine = this.engine;
        if (oldEngine != null) {
            oldEngine.setService(null);
        }
        this.engine = engine;
        if (this.engine != null) {
            this.engine.setService(this);
        }
        if (getState().isAvailable()) {
            if (this.engine != null) {
                try {
                    this.engine.start();
                } catch (LifecycleException e) {
                    log.error(sm.getString("standardService.engine.startFailed"), e);
                }
            }
            // Restart MapperListener to pick up new engine.
            try {
                mapperListener.stop();
            } catch (LifecycleException e) {
                log.error(sm.getString("standardService.mapperListener.stopFailed"), e);
            }
            try {
                mapperListener.start();
            } catch (LifecycleException e) {
                log.error(sm.getString("standardService.mapperListener.startFailed"), e);
            }
            if (oldEngine != null) {
                try {
                    oldEngine.stop();
                } catch (LifecycleException e) {
                    log.error(sm.getString("standardService.engine.stopFailed"), e);
                }
            }
        }

        // Report this property change to interested listeners
        support.firePropertyChange("container", oldEngine, this.engine);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public Server getServer() {
        return this.server;
    }

    @Override
    public void setServer(Server server) {
        this.server = server;
    }

    // --------------------------------------------------------- Public Methods

    @Override
    public void addConnector(Connector connector) {

        Lock writeLock = connectorsLock.writeLock();
        writeLock.lock();
        try {
            connector.setService(this);
            Connector[] results = new Connector[connectors.length + 1];
            System.arraycopy(connectors, 0, results, 0, connectors.length);
            results[connectors.length] = connector;
            connectors = results;
        } finally {
            writeLock.unlock();
        }

        try {
            if (getState().isAvailable()) {
                connector.start();
            }
        } catch (LifecycleException e) {
            throw new IllegalArgumentException(
                    sm.getString("standardService.connector.startFailed", connector), e);
        }

        // Report this property change to interested listeners
        support.firePropertyChange("connector", null, connector);
    }

    /**
     * Add a property change listener to this component.
     *
     * @param listener The listener to add
     */
    public void addPropertyChangeListener(PropertyChangeListener listener) {
        support.addPropertyChangeListener(listener);
    }

    @Override
    public Connector[] findConnectors() {
        Lock readLock = connectorsLock.readLock();
        readLock.lock();
        try {
            // shallow copy
            return connectors.clone();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void removeConnector(Connector connector) {

        Lock writeLock = connectorsLock.writeLock();
        writeLock.lock();
        try {
            int j = -1;
            for (int i = 0; i < connectors.length; i++) {
                if (connector == connectors[i]) {
                    j = i;
                    break;
                }
            }
            if (j < 0) {
                return;
            }
            int k = 0;
            Connector[] results = new Connector[connectors.length - 1];
            for (int i = 0; i < connectors.length; i++) {
                if (i != j) {
                    results[k++] = connectors[i];
                }
            }
            connectors = results;

        } finally {
            writeLock.unlock();
        }

        if (connector.getState().isAvailable()) {
            try {
                connector.stop();
            } catch (LifecycleException e) {
                log.error(sm.getString("standardService.connector.stopFailed", connector), e);
            }
        }
        connector.setService(null);

        // Report this property change to interested listeners
        support.firePropertyChange("connector", connector, null);
    }

    /**
     * Remove a property change listener from this component.
     *
     * @param listener The listener to remove
     */
    public void removePropertyChangeListener(PropertyChangeListener listener) {
        support.removePropertyChangeListener(listener);
    }

    @Override
    public String toString() {
        return "StandardService[" + getName() + "]";
    }

    /**
     * Start nested components ({@link Executor}s, {@link Connector}s and {@link Container}s) and implement the
     * requirements of {@link com.lealone.server.http.container.util.LifecycleBase#startInternal()}.
     *
     * @exception LifecycleException if this component detects a fatal error that prevents this component from being
     *                                   used
     */
    @Override
    protected void startInternal() throws LifecycleException {
        // if (log.isInfoEnabled()) {
        // log.info(sm.getString("standardService.start.name", this.name));
        // }
        setState(LifecycleState.STARTING);

        // Start our defined Container first
        if (engine != null) {
            engine.start();
        }

        mapperListener.start();

        // Start our defined Connectors second
        for (Connector connector : findConnectors()) {
            // If it has already failed, don't try and start it
            if (connector.getState() != LifecycleState.FAILED) {
                connector.start();
            }
        }
    }

    /**
     * Stop nested components ({@link Executor}s, {@link Connector}s and {@link Container}s) and implement the
     * requirements of {@link com.lealone.server.http.container.util.LifecycleBase#stopInternal()}.
     *
     * @exception LifecycleException if this component detects a fatal error that needs to be reported
     */
    @Override
    protected void stopInternal() throws LifecycleException {

        Connector[] connectors = findConnectors();
        // Initiate a graceful stop for each connector
        // This will only work if the bindOnInit==false which is not the
        // default.
        for (Connector connector : connectors) {
            connector.getProtocolHandler().closeServerSocketGraceful();
        }

        // Wait for the graceful shutdown to complete
        long waitMillis = gracefulStopAwaitMillis;
        if (waitMillis > 0) {
            for (Connector connector : connectors) {
                waitMillis = connector.getProtocolHandler().awaitConnectionsClose(waitMillis);
            }
        }

        // Pause the connectors
        for (Connector connector : connectors) {
            connector.pause();
        }

        // if (log.isInfoEnabled()) {
        // log.info(sm.getString("standardService.stop.name", this.name));
        // }
        setState(LifecycleState.STOPPING);

        // Stop our defined Container once the Connectors are all paused
        if (engine != null) {
            engine.stop();
        }

        // Now stop the connectors
        for (Connector connector : connectors) {
            if (!LifecycleState.STARTED.equals(connector.getState())) {
                // Connectors only need stopping if they are currently
                // started. They may have failed to start or may have been
                // stopped (e.g. via a JMX call)
                continue;
            }
            connector.stop();
        }

        // If the Server failed to start, the mapperListener won't have been
        // started
        if (mapperListener.getState() != LifecycleState.INITIALIZED) {
            mapperListener.stop();
        }
    }

    /**
     * Invoke a pre-startup initialization. This is used to allow connectors to bind to restricted ports under Unix
     * operating environments.
     *
     * @exception LifecycleException if this component detects a fatal error that needs to be reported
     */
    @Override
    protected void initInternal() throws LifecycleException {

        super.initInternal();

        if (engine != null) {
            engine.init();
        }

        // Initialize mapper listener
        mapperListener.init();

        // Initialize our defined Connectors
        for (Connector connector : findConnectors()) {
            connector.init();
        }
    }

    @Override
    protected void destroyInternal() throws LifecycleException {
        mapperListener.destroy();

        // Destroy our defined Connectors
        for (Connector connector : findConnectors()) {
            connector.destroy();
        }

        if (engine != null) {
            engine.destroy();
        }

        super.destroyInternal();
    }
}
