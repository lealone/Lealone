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

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

import com.lealone.server.http.container.AccessLog;
import com.lealone.server.http.container.Container;
import com.lealone.server.http.container.ContainerEvent;
import com.lealone.server.http.container.ContainerListener;
import com.lealone.server.http.container.Context;
import com.lealone.server.http.container.Engine;
import com.lealone.server.http.container.Host;
import com.lealone.server.http.container.LifecycleEvent;
import com.lealone.server.http.container.LifecycleException;
import com.lealone.server.http.container.LifecycleListener;
import com.lealone.server.http.container.Realm;
import com.lealone.server.http.container.Server;
import com.lealone.server.http.container.Service;
import com.lealone.server.http.container.connector.Request;
import com.lealone.server.http.container.connector.Response;
import com.lealone.server.http.container.realm.NullRealm;

/**
 * Standard implementation of the <b>Engine</b> interface. Each child container must be a Host implementation to process
 * the specific fully qualified host name of that virtual host.
 */
public class StandardEngine extends ContainerBase implements Engine {

    // private static final Logger log = LoggerFactory.getLogger(StandardEngine.class);

    // ----------------------------------------------------------- Constructors

    /**
     * Create a new StandardEngine component with the default basic Valve.
     */
    public StandardEngine() {
        pipeline.setBasic(new StandardEngineValve());
        // By default, the engine will hold the reloading thread
        backgroundProcessorDelay = 10;
    }

    // ----------------------------------------------------- Instance Variables

    /**
     * Host name to use when no server host, or an unknown host, is specified in the request.
     */
    private String defaultHost = null;

    /**
     * The <code>Service</code> that owns this Engine, if any.
     */
    private Service service = null;

    /**
     * The JVM Route ID for this Tomcat instance. All Route ID's must be unique across the cluster.
     */
    private String jvmRouteId;

    /**
     * Default access log to use for request/response pairs where we can't ID the intended host and context.
     */
    private final AtomicReference<AccessLog> defaultAccessLog = new AtomicReference<>();

    // ------------------------------------------------------------- Properties

    @Override
    public Realm getRealm() {
        Realm configured = super.getRealm();
        // If no set realm has been called - default to NullRealm
        // This can be overridden at engine, context and host level
        if (configured == null) {
            configured = new NullRealm();
            this.setRealm(configured);
        }
        return configured;
    }

    @Override
    public String getDefaultHost() {
        return defaultHost;
    }

    @Override
    public void setDefaultHost(String host) {

        String oldDefaultHost = this.defaultHost;
        if (host == null) {
            this.defaultHost = null;
        } else {
            this.defaultHost = host.toLowerCase(Locale.ENGLISH);
        }
        if (getState().isAvailable()) {
            service.getMapper().setDefaultHostName(host);
        }
        support.firePropertyChange("defaultHost", oldDefaultHost, this.defaultHost);

    }

    @Override
    public void setJvmRoute(String routeId) {
        jvmRouteId = routeId;
    }

    @Override
    public String getJvmRoute() {
        return jvmRouteId;
    }

    @Override
    public Service getService() {
        return this.service;
    }

    @Override
    public void setService(Service service) {
        this.service = service;
    }

    // --------------------------------------------------------- Public Methods

    /**
     * {@inheritDoc}
     * <p>
     * The child must be an implementation of <code>Host</code>.
     */
    @Override
    public void addChild(Container child) {

        if (!(child instanceof Host)) {
            throw new IllegalArgumentException(sm.getString("standardEngine.notHost"));
        }
        super.addChild(child);

    }

    /**
     * Disallow any attempt to set a parent for this Container, since an Engine is supposed to be at the top of the
     * Container hierarchy.
     *
     * @param container Proposed parent Container
     */
    @Override
    public void setParent(Container container) {

        throw new IllegalArgumentException(sm.getString("standardEngine.notParent"));

    }

    @Override
    protected void initInternal() throws LifecycleException {
        // Ensure that a Realm is present before any attempt is made to start
        // one. This will create the default NullRealm if necessary.
        getRealm();
        super.initInternal();
    }

    @Override
    protected void startInternal() throws LifecycleException {

        // Log our server identification information
        // if (log.isInfoEnabled()) {
        // log.info(sm.getString("standardEngine.start", ServerInfo.getServerInfo()));
        // }

        // Standard container startup
        super.startInternal();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Override the default implementation. If no access log is defined for the Engine, look for one in the Engine's
     * default host and then the default host's ROOT context. If still none is found, return the default NoOp access
     * log.
     */
    @Override
    public void logAccess(Request request, Response response, long time, boolean useDefault) {

        boolean logged = false;

        if (getAccessLog() != null) {
            accessLog.log(request, response, time);
            logged = true;
        }

        if (!logged && useDefault) {
            AccessLog newDefaultAccessLog = defaultAccessLog.get();
            if (newDefaultAccessLog == null) {
                // If we reached this point, this Engine can't have an AccessLog
                // Look in the defaultHost
                Host host = (Host) findChild(getDefaultHost());
                Context context = null;
                if (host != null && host.getState().isAvailable()) {
                    newDefaultAccessLog = host.getAccessLog();

                    if (newDefaultAccessLog != null) {
                        if (defaultAccessLog.compareAndSet(null, newDefaultAccessLog)) {
                            AccessLogListener l = new AccessLogListener(this, host, null);
                            l.install();
                        }
                    } else {
                        // Try the ROOT context of default host
                        context = (Context) host.findChild("");
                        if (context != null && context.getState().isAvailable()) {
                            newDefaultAccessLog = context.getAccessLog();
                            if (newDefaultAccessLog != null) {
                                if (defaultAccessLog.compareAndSet(null, newDefaultAccessLog)) {
                                    AccessLogListener l = new AccessLogListener(this, null, context);
                                    l.install();
                                }
                            }
                        }
                    }
                }

                if (newDefaultAccessLog == null) {
                    newDefaultAccessLog = new NoopAccessLog();
                    if (defaultAccessLog.compareAndSet(null, newDefaultAccessLog)) {
                        AccessLogListener l = new AccessLogListener(this, host, context);
                        l.install();
                    }
                }
            }

            newDefaultAccessLog.log(request, response, time);
        }
    }

    @Override
    public File getBaseDir() {
        if (service != null) {
            Server s = service.getServer();
            if (s != null) {
                File base = s.getBaseDir();
                if (base != null) {
                    return base;
                }
            }
        }
        // Fall-back
        return super.getBaseDir();
    }

    // ----------------------------------------------------------- Inner classes
    protected static final class NoopAccessLog implements AccessLog {

        @Override
        public void log(Request request, Response response, long time) {
            // NOOP
        }

        @Override
        public void setRequestAttributesEnabled(boolean requestAttributesEnabled) {
            // NOOP

        }

        @Override
        public boolean getRequestAttributesEnabled() {
            // NOOP
            return false;
        }
    }

    protected static final class AccessLogListener
            implements PropertyChangeListener, LifecycleListener, ContainerListener {

        private final StandardEngine engine;
        private final Host host;
        private final Context context;
        private volatile boolean disabled = false;

        public AccessLogListener(StandardEngine engine, Host host, Context context) {
            this.engine = engine;
            this.host = host;
            this.context = context;
        }

        public void install() {
            engine.addPropertyChangeListener(this);
            if (host != null) {
                host.addContainerListener(this);
                host.addLifecycleListener(this);
            }
            if (context != null) {
                context.addLifecycleListener(this);
            }
        }

        private void uninstall() {
            disabled = true;
            if (context != null) {
                context.removeLifecycleListener(this);
            }
            if (host != null) {
                host.removeLifecycleListener(this);
                host.removeContainerListener(this);
            }
            engine.removePropertyChangeListener(this);
        }

        @Override
        public void lifecycleEvent(LifecycleEvent event) {
            if (disabled) {
                return;
            }

            String type = event.getType();
            if (AFTER_START_EVENT.equals(type) || BEFORE_STOP_EVENT.equals(type)
                    || BEFORE_DESTROY_EVENT.equals(type)) {
                // Container is being started/stopped/removed
                // Force re-calculation and disable listener since it won't
                // be re-used
                engine.defaultAccessLog.set(null);
                uninstall();
            }
        }

        @Override
        public void propertyChange(PropertyChangeEvent evt) {
            if (disabled) {
                return;
            }
            if ("defaultHost".equals(evt.getPropertyName())) {
                // Force re-calculation and disable listener since it won't
                // be re-used
                engine.defaultAccessLog.set(null);
                uninstall();
            }
        }

        @Override
        public void containerEvent(ContainerEvent event) {
            // Only useful for hosts
            if (disabled) {
                return;
            }
            if (ADD_CHILD_EVENT.equals(event.getType())) {
                Context context = (Context) event.getData();
                if (context.getPath().isEmpty()) {
                    // Force re-calculation and disable listener since it won't
                    // be re-used
                    engine.defaultAccessLog.set(null);
                    uninstall();
                }
            }
        }
    }
}
