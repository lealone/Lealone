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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.lealone.db.async.AsyncPeriodicTask;
import com.lealone.db.scheduler.SchedulerThread;
import com.lealone.server.http.container.LifecycleException;
import com.lealone.server.http.container.LifecycleState;
import com.lealone.server.http.container.Server;
import com.lealone.server.http.container.Service;
import com.lealone.server.http.container.util.LifecycleMBeanBase;
import com.lealone.server.http.container.util.ServerInfo;

/**
 * Standard implementation of the <b>Server</b> interface, available for use (but not required) when deploying and
 * starting Catalina.
 */
public final class StandardServer extends LifecycleMBeanBase implements Server {

    // ------------------------------------------------------------ Constructor

    /**
     * Construct a default instance of this class.
     */
    public StandardServer() {
    }

    // ----------------------------------------------------- Instance Variables

    /**
     * The set of Services associated with this Server.
     */
    private Service[] services = new Service[0];

    private final ReentrantReadWriteLock servicesLock = new ReentrantReadWriteLock();
    private final Lock servicesReadLock = servicesLock.readLock();
    private final Lock servicesWriteLock = servicesLock.writeLock();

    /**
     * The property change support for this component.
     */
    private final PropertyChangeSupport support = new PropertyChangeSupport(this);

    private File baseDir = null;

    /**
     * Controller for the periodic lifecycle event.
     */
    private AsyncPeriodicTask asyncPeriodicTask;

    /**
     * The lifecycle event period in seconds.
     */
    private int periodicEventDelay = 10;

    // ------------------------------------------------------------- Properties

    /**
     * Report the current Tomcat Server Release number
     *
     * @return Tomcat release identifier
     */
    public String getServerInfo() {
        return ServerInfo.getServerInfo();
    }

    /**
     * @return The period between two lifecycle events, in seconds
     */
    public int getPeriodicEventDelay() {
        return periodicEventDelay;
    }

    /**
     * Set the new period between two lifecycle events in seconds.
     *
     * @param periodicEventDelay The period in seconds, negative or zero will disable events
     */
    public void setPeriodicEventDelay(int periodicEventDelay) {
        this.periodicEventDelay = periodicEventDelay;
    }

    // --------------------------------------------------------- Server Methods

    @Override
    public void addService(Service service) {

        service.setServer(this);

        servicesWriteLock.lock();
        try {
            Service[] results = new Service[services.length + 1];
            System.arraycopy(services, 0, results, 0, services.length);
            results[services.length] = service;
            services = results;
        } finally {
            servicesWriteLock.unlock();
        }

        if (getState().isAvailable()) {
            try {
                service.start();
            } catch (LifecycleException e) {
                // Ignore
            }
        }

        // Report this property change to interested listeners
        support.firePropertyChange("service", null, service);
    }

    @Override
    public Service findService(String name) {
        if (name == null) {
            return null;
        }
        servicesReadLock.lock();

        try {
            for (Service service : services) {
                if (name.equals(service.getName())) {
                    return service;
                }
            }
        } finally {
            servicesReadLock.unlock();
        }

        return null;
    }

    @Override
    public Service[] findServices() {
        servicesReadLock.lock();
        try {
            return services.clone();
        } finally {
            servicesReadLock.unlock();
        }
    }

    @Override
    public void removeService(Service service) {

        servicesWriteLock.lock();

        try {
            int j = -1;
            for (int i = 0; i < services.length; i++) {
                if (service == services[i]) {
                    j = i;
                    break;
                }
            }
            if (j < 0) {
                return;
            }
            int k = 0;
            Service[] results = new Service[services.length - 1];
            for (int i = 0; i < services.length; i++) {
                if (i != j) {
                    results[k++] = services[i];
                }
            }
            services = results;
        } finally {
            servicesWriteLock.unlock();
        }

        try {
            service.stop();
        } catch (LifecycleException e) {
            // Ignore
        }

        // Report this property change to interested listeners
        support.firePropertyChange("service", service, null);
    }

    @Override
    public File getBaseDir() {
        return baseDir;
    }

    @Override
    public void setBaseDir(File baseDir) {
        this.baseDir = baseDir;
    }

    // --------------------------------------------------------- Public Methods

    /**
     * Add a property change listener to this component.
     *
     * @param listener The listener to add
     */
    public void addPropertyChangeListener(PropertyChangeListener listener) {

        support.addPropertyChangeListener(listener);

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
    protected void startInternal() throws LifecycleException {

        fireLifecycleEvent(CONFIGURE_START_EVENT, null);
        setState(LifecycleState.STARTING);

        // Start our defined Services
        for (Service service : findServices()) {
            service.start();
        }

        if (periodicEventDelay > 0 && SchedulerThread.currentScheduler() != null) {
            asyncPeriodicTask = new AsyncPeriodicTask(periodicEventDelay * 1000, 60 * 1000,
                    () -> fireLifecycleEvent(PERIODIC_EVENT, null));
            SchedulerThread.currentScheduler().addPeriodicTask(asyncPeriodicTask);
        }
    }

    @Override
    protected void stopInternal() throws LifecycleException {

        setState(LifecycleState.STOPPING);

        if (asyncPeriodicTask != null) {
            asyncPeriodicTask.cancel();
            asyncPeriodicTask = null;
        }

        fireLifecycleEvent(CONFIGURE_STOP_EVENT, null);

        // Stop our defined Services
        for (Service service : findServices()) {
            service.stop();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * This is used to allow connectors to bind to restricted ports under Unix operating environments.
     */
    @Override
    protected void initInternal() throws LifecycleException {

        super.initInternal();

        // Initialize our defined Services
        for (Service service : findServices()) {
            service.init();
        }
    }

    @Override
    protected void destroyInternal() throws LifecycleException {
        // Destroy our defined Services
        for (Service service : findServices()) {
            service.destroy();
        }

        super.destroyInternal();
    }
}
