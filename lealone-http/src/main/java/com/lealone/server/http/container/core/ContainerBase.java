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
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.db.async.AsyncPeriodicTask;
import com.lealone.db.scheduler.SchedulerThread;
import com.lealone.server.http.container.AccessLog;
import com.lealone.server.http.container.Container;
import com.lealone.server.http.container.ContainerEvent;
import com.lealone.server.http.container.ContainerListener;
import com.lealone.server.http.container.Context;
import com.lealone.server.http.container.Lifecycle;
import com.lealone.server.http.container.LifecycleException;
import com.lealone.server.http.container.LifecycleState;
import com.lealone.server.http.container.Pipeline;
import com.lealone.server.http.container.Realm;
import com.lealone.server.http.container.Valve;
import com.lealone.server.http.container.connector.Request;
import com.lealone.server.http.container.connector.Response;
import com.lealone.server.http.container.util.LifecycleMBeanBase;
import com.lealone.server.http.util.ExceptionUtils;
import com.lealone.server.http.util.res.StringManager;

/**
 * Abstract implementation of the <b>Container</b> interface, providing common functionality required by nearly every
 * implementation. Classes extending this base class must may implement a replacement for <code>invoke()</code>.
 * <p>
 * All subclasses of this abstract base class will include support for a Pipeline object that defines the processing to
 * be performed for each request received by the <code>invoke()</code> method of this class, utilizing the "Chain of
 * Responsibility" design pattern. A subclass should encapsulate its own processing functionality as a
 * <code>Valve</code>, and configure this Valve into the pipeline by calling <code>setBasic()</code>.
 * <p>
 * This implementation fires property change events, per the JavaBeans design pattern, for changes in singleton
 * properties. In addition, it fires the following <code>ContainerEvent</code> events to listeners who register
 * themselves with <code>addContainerListener()</code>:
 * <table border=1>
 * <caption>ContainerEvents fired by this implementation</caption>
 * <tr>
 * <th>Type</th>
 * <th>Data</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td><code>addChild</code></td>
 * <td><code>Container</code></td>
 * <td>Child container added to this Container.</td>
 * </tr>
 * <tr>
 * <td><code>{@link #getPipeline() pipeline}.addValve</code></td>
 * <td><code>Valve</code></td>
 * <td>Valve added to this Container.</td>
 * </tr>
 * <tr>
 * <td><code>removeChild</code></td>
 * <td><code>Container</code></td>
 * <td>Child container removed from this Container.</td>
 * </tr>
 * <tr>
 * <td><code>{@link #getPipeline() pipeline}.removeValve</code></td>
 * <td><code>Valve</code></td>
 * <td>Valve removed from this Container.</td>
 * </tr>
 * <tr>
 * <td><code>start</code></td>
 * <td><code>null</code></td>
 * <td>Container was started.</td>
 * </tr>
 * <tr>
 * <td><code>stop</code></td>
 * <td><code>null</code></td>
 * <td>Container was stopped.</td>
 * </tr>
 * </table>
 * Subclasses that fire additional events should document them in the class comments of the implementation class.
 */
public abstract class ContainerBase extends LifecycleMBeanBase implements Container {

    private static final Logger log = LoggerFactory.getLogger(ContainerBase.class);

    // ----------------------------------------------------- Instance Variables

    /**
     * The child Containers belonging to this Container, keyed by name.
     */
    protected final HashMap<String, Container> children = new HashMap<>();
    private final ReadWriteLock childrenLock = new ReentrantReadWriteLock();

    /**
     * The processor delay for this component.
     */
    protected int backgroundProcessorDelay = -1;

    /**
     * The future allowing control of the background processor.
     */
    protected AsyncPeriodicTask asyncPeriodicTask;

    /**
     * The container event listeners for this Container. Implemented as a CopyOnWriteArrayList since listeners may
     * invoke methods to add/remove themselves or other listeners and with a ReadWriteLock that would trigger a
     * deadlock.
     */
    protected final List<ContainerListener> listeners = new CopyOnWriteArrayList<>();

    /**
     * The Logger implementation with which this Container is associated.
     */
    protected Logger logger = null;

    /**
     * Associated logger name.
     */
    protected String logName = null;

    /**
     * The human-readable name of this Container.
     */
    protected String name = null;

    /**
     * The parent Container to which this Container is a child.
     */
    protected Container parent = null;

    /**
     * The Pipeline object with which this Container is associated.
     */
    protected final Pipeline pipeline = new StandardPipeline(this);

    /**
     * The Realm with which this Container is associated.
     */
    private volatile Realm realm = null;

    /**
     * Lock used to control access to the Realm.
     */
    private final ReadWriteLock realmLock = new ReentrantReadWriteLock();

    /**
     * The string manager for this package.
     */
    protected static final StringManager sm = StringManager.getManager(ContainerBase.class);

    /**
     * Will children be started automatically when they are added.
     */
    protected boolean startChildren = true;

    /**
     * The property change support for this component.
     */
    protected final PropertyChangeSupport support = new PropertyChangeSupport(this);

    /**
     * The access log to use for requests normally handled by this container that have been handled earlier in the
     * processing chain.
     */
    protected volatile AccessLog accessLog = null;
    private volatile boolean accessLogScanComplete = false;

    // ------------------------------------------------------------- Properties

    @Override
    public int getBackgroundProcessorDelay() {
        return backgroundProcessorDelay;
    }

    @Override
    public void setBackgroundProcessorDelay(int delay) {
        backgroundProcessorDelay = delay;
    }

    @Override
    public Logger getLogger() {
        if (logger != null) {
            return logger;
        }
        logger = LoggerFactory.getLogger(getLogName());
        return logger;
    }

    @Override
    public String getLogName() {

        if (logName != null) {
            return logName;
        }
        String loggerName = null;
        Container current = this;
        while (current != null) {
            String name = current.getName();
            if ((name == null) || (name.isEmpty())) {
                name = "/";
            } else if (name.startsWith("##")) {
                name = "/" + name;
            }
            loggerName = "[" + name + "]" + ((loggerName != null) ? ("." + loggerName) : "");
            current = current.getParent();
        }
        logName = ContainerBase.class.getName() + "." + loggerName;
        return logName;

    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        if (name == null) {
            throw new IllegalArgumentException(sm.getString("containerBase.nullName"));
        }
        String oldName = this.name;
        this.name = name;
        support.firePropertyChange("name", oldName, this.name);
    }

    /**
     * Return if children of this container will be started automatically when they are added to this container.
     *
     * @return <code>true</code> if the children will be started
     */
    public boolean getStartChildren() {
        return startChildren;
    }

    /**
     * Set if children of this container will be started automatically when they are added to this container.
     *
     * @param startChildren New value of the startChildren flag
     */
    public void setStartChildren(boolean startChildren) {

        boolean oldStartChildren = this.startChildren;
        this.startChildren = startChildren;
        support.firePropertyChange("startChildren", oldStartChildren, this.startChildren);
    }

    @Override
    public Container getParent() {
        return parent;
    }

    @Override
    public void setParent(Container container) {

        Container oldParent = this.parent;
        this.parent = container;
        support.firePropertyChange("parent", oldParent, this.parent);

    }

    @Override
    public Pipeline getPipeline() {
        return this.pipeline;
    }

    @Override
    public Realm getRealm() {

        Lock l = realmLock.readLock();
        l.lock();
        try {
            if (realm != null) {
                return realm;
            }
            if (parent != null) {
                return parent.getRealm();
            }
            return null;
        } finally {
            l.unlock();
        }
    }

    protected Realm getRealmInternal() {
        Lock l = realmLock.readLock();
        l.lock();
        try {
            return realm;
        } finally {
            l.unlock();
        }
    }

    @Override
    public void setRealm(Realm realm) {

        Realm oldRealm;
        Lock l = realmLock.writeLock();
        l.lock();
        try {
            // Change components if necessary
            oldRealm = this.realm;
            if (oldRealm == realm) {
                return;
            }
            this.realm = realm;

            // Start the new component if necessary
            if (realm != null) {
                realm.setContainer(this);
            }
        } finally {
            l.unlock();
        }

        // Stop the old component if necessary
        if (getState().isAvailable() && oldRealm instanceof Lifecycle) {
            try {
                ((Lifecycle) oldRealm).stop();
            } catch (LifecycleException e) {
                log.error(sm.getString("containerBase.realm.stop"), e);
            }
        }

        if (getState().isAvailable() && realm instanceof Lifecycle) {
            try {
                ((Lifecycle) realm).start();
            } catch (LifecycleException e) {
                log.error(sm.getString("containerBase.realm.start"), e);
            }
        }

        // Report this property change to interested listeners
        support.firePropertyChange("realm", oldRealm, this.realm);
    }

    // ------------------------------------------------------ Container Methods

    @Override
    public void addChild(Container child) {
        if (log.isDebugEnabled()) {
            log.debug(sm.getString("containerBase.child.add", child, this));
        }

        childrenLock.writeLock().lock();
        try {
            if (children.get(child.getName()) != null) {
                throw new IllegalArgumentException(
                        sm.getString("containerBase.child.notUnique", child.getName()));
            }
            child.setParent(this); // May throw IAE
            children.put(child.getName(), child);
        } finally {
            childrenLock.writeLock().unlock();
        }

        fireContainerEvent(ADD_CHILD_EVENT, child);

        // Start child
        // Don't do this inside sync block - start can be a slow process and
        // locking the children object can cause problems elsewhere
        try {
            if ((getState().isAvailable() || LifecycleState.STARTING_PREP.equals(getState()))
                    && startChildren) {
                child.start();
            }
        } catch (LifecycleException e) {
            throw new IllegalStateException(sm.getString("containerBase.child.start"), e);
        }
    }

    @Override
    public void addContainerListener(ContainerListener listener) {
        listeners.add(listener);
    }

    @Override
    public void addPropertyChangeListener(PropertyChangeListener listener) {
        support.addPropertyChangeListener(listener);
    }

    @Override
    public Container findChild(String name) {
        if (name == null) {
            return null;
        }
        childrenLock.readLock().lock();
        try {
            return children.get(name);
        } finally {
            childrenLock.readLock().unlock();
        }
    }

    @Override
    public Container[] findChildren() {
        childrenLock.readLock().lock();
        try {
            return children.values().toArray(new Container[0]);
        } finally {
            childrenLock.readLock().unlock();
        }
    }

    @Override
    public ContainerListener[] findContainerListeners() {
        return listeners.toArray(new ContainerListener[0]);
    }

    @Override
    public void removeChild(Container child) {

        if (child == null) {
            return;
        }

        try {
            if (child.getState().isAvailable()) {
                child.stop();
            }
        } catch (LifecycleException e) {
            log.error(sm.getString("containerBase.child.stop"), e);
        }

        boolean destroy = false;
        try {
            // child.destroy() may have already been called which would have
            // triggered this call. If that is the case, no need to destroy the
            // child again.
            if (!LifecycleState.DESTROYING.equals(child.getState())) {
                child.destroy();
                destroy = true;
            }
        } catch (LifecycleException e) {
            log.error(sm.getString("containerBase.child.destroy"), e);
        }

        if (!destroy) {
            fireContainerEvent(REMOVE_CHILD_EVENT, child);
        }

        childrenLock.writeLock().lock();
        try {
            children.remove(child.getName());
        } finally {
            childrenLock.writeLock().unlock();
        }

    }

    @Override
    public void removeContainerListener(ContainerListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void removePropertyChangeListener(PropertyChangeListener listener) {

        support.removePropertyChangeListener(listener);

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
        // Start our subordinate components, if any
        logger = null;
        getLogger();
        Realm realm = getRealmInternal();
        if (realm instanceof Lifecycle) {
            ((Lifecycle) realm).start();
        }

        // Start our child containers, if any
        Container[] children = findChildren();
        for (Container child : children) {
            child.start();
        }

        // Start the Valves in our pipeline (including the basic), if any
        if (pipeline instanceof Lifecycle) {
            ((Lifecycle) pipeline).start();
        }

        setState(LifecycleState.STARTING);

        if (backgroundProcessorDelay > 0 && SchedulerThread.currentScheduler() != null) {
            asyncPeriodicTask = new AsyncPeriodicTask(backgroundProcessorDelay * 1000, 60 * 1000,
                    new ContainerBackgroundProcessor());
            SchedulerThread.currentScheduler().addPeriodicTask(asyncPeriodicTask);
        }
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

        if (asyncPeriodicTask != null) {
            asyncPeriodicTask.cancel();
            asyncPeriodicTask = null;
        }

        setState(LifecycleState.STOPPING);

        // Stop the Valves in our pipeline (including the basic), if any
        if (pipeline instanceof Lifecycle && ((Lifecycle) pipeline).getState().isAvailable()) {
            ((Lifecycle) pipeline).stop();
        }

        // Stop our child containers, if any
        Container[] children = findChildren();
        for (Container child : children) {
            child.stop();
        }

        // Stop our subordinate components, if any
        Realm realm = getRealmInternal();
        if (realm instanceof Lifecycle) {
            ((Lifecycle) realm).stop();
        }
    }

    @Override
    protected void destroyInternal() throws LifecycleException {

        Realm realm = getRealmInternal();
        if (realm instanceof Lifecycle) {
            ((Lifecycle) realm).destroy();
        }
        // Stop the Valves in our pipeline (including the basic), if any
        if (pipeline instanceof Lifecycle) {
            ((Lifecycle) pipeline).destroy();
        }

        // Remove children now this container is being destroyed
        for (Container child : findChildren()) {
            removeChild(child);
        }

        // Required if the child is destroyed directly.
        if (parent != null) {
            parent.removeChild(this);
        }

        super.destroyInternal();
    }

    @Override
    public void logAccess(Request request, Response response, long time, boolean useDefault) {

        boolean logged = false;

        if (getAccessLog() != null) {
            getAccessLog().log(request, response, time);
            logged = true;
        }

        if (getParent() != null) {
            // No need to use default logger once request/response has been logged
            // once
            getParent().logAccess(request, response, time, (useDefault && !logged));
        }
    }

    @Override
    public AccessLog getAccessLog() {

        if (accessLogScanComplete) {
            return accessLog;
        }

        AccessLogAdapter adapter = null;
        Valve[] valves = getPipeline().getValves();
        for (Valve valve : valves) {
            if (valve instanceof AccessLog) {
                if (adapter == null) {
                    adapter = new AccessLogAdapter((AccessLog) valve);
                } else {
                    adapter.add((AccessLog) valve);
                }
            }
        }
        if (adapter != null) {
            accessLog = adapter;
        }
        accessLogScanComplete = true;
        return accessLog;
    }

    // ------------------------------------------------------- Pipeline Methods

    /**
     * Convenience method, intended for use by the digester to simplify the process of adding Valves to containers. See
     * {@link Pipeline#addValve(Valve)} for full details. Components other than the digester should use
     * {@link #getPipeline()}.{@link #addValve(Valve)} in case a future implementation provides an alternative method
     * for the digester to use.
     *
     * @param valve Valve to be added
     *
     * @exception IllegalArgumentException if this Container refused to accept the specified Valve
     * @exception IllegalArgumentException if the specified Valve refuses to be associated with this Container
     * @exception IllegalStateException    if the specified Valve is already associated with a different Container
     */
    public synchronized void addValve(Valve valve) {

        pipeline.addValve(valve);
    }

    @Override
    public synchronized void backgroundProcess() {

        if (!getState().isAvailable()) {
            return;
        }

        Realm realm = getRealmInternal();
        if (realm != null) {
            try {
                realm.backgroundProcess();
            } catch (Exception e) {
                log.warn(sm.getString("containerBase.backgroundProcess.realm", realm), e);
            }
        }
        Valve current = pipeline.getFirst();
        while (current != null) {
            try {
                current.backgroundProcess();
            } catch (Exception e) {
                log.warn(sm.getString("containerBase.backgroundProcess.valve", current), e);
            }
            current = current.getNext();
        }
        fireLifecycleEvent(PERIODIC_EVENT, null);
    }

    @Override
    public File getBaseDir() {
        if (parent == null) {
            return null;
        }
        return parent.getBaseDir();
    }

    // ------------------------------------------------------ Protected Methods

    @Override
    public void fireContainerEvent(String type, Object data) {

        if (listeners.isEmpty()) {
            return;
        }

        ContainerEvent event = new ContainerEvent(this, type, data);
        // Note for each uses an iterator internally so this is safe
        for (ContainerListener listener : listeners) {
            listener.containerEvent(event);
        }
    }

    @Override
    public final String toString() {
        StringBuilder sb = new StringBuilder();
        Container parent = getParent();
        if (parent != null) {
            sb.append(parent);
            sb.append('.');
        }
        sb.append(this.getClass().getSimpleName());
        sb.append('[');
        sb.append(getName());
        sb.append(']');
        return sb.toString();
    }

    /**
     * Private runnable class to invoke the backgroundProcess method of this container and its children after a fixed
     * delay.
     */
    protected class ContainerBackgroundProcessor implements Runnable {

        @Override
        public void run() {
            processChildren(ContainerBase.this);
        }

        protected void processChildren(Container container) {
            ClassLoader originalClassLoader = null;

            try {
                if (container instanceof Context) {
                    // Ensure background processing for Contexts and Wrappers
                    // is performed under the web app's class loader
                    originalClassLoader = ((Context) container).bind(null);
                }
                container.backgroundProcess();
                Container[] children = container.findChildren();
                for (Container child : children) {
                    if (child.getBackgroundProcessorDelay() <= 0) {
                        processChildren(child);
                    }
                }
            } catch (Throwable t) {
                ExceptionUtils.handleThrowable(t);
                log.error(sm.getString("containerBase.backgroundProcess.error"), t);
            } finally {
                if (container instanceof Context) {
                    ((Context) container).unbind(originalClassLoader);
                }
            }
        }
    }
}
