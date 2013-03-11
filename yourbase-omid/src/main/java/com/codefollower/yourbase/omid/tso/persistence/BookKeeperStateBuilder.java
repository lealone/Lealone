/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. See accompanying LICENSE file.
 */

package com.codefollower.yourbase.omid.tso.persistence;

import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.codefollower.yourbase.omid.tso.TSOServerConfig;
import com.codefollower.yourbase.omid.tso.TSOState;
import com.codefollower.yourbase.omid.tso.TimestampOracle;
import com.codefollower.yourbase.omid.tso.persistence.LoggerAsyncCallback.LoggerInitCallback;
import com.codefollower.yourbase.omid.tso.persistence.LoggerException.Code;

/**
 * Builds the TSO state from a BookKeeper ledger if there has been a previous 
 * incarnation of TSO. Note that we need to make sure that the zookeeper session 
 * is the same across builder and logger, so we create in builder and pass it
 * to logger. This is the case to prevent two TSO instances from creating a lock
 * and updating the ledger id after having lost the lock. This case constitutes
 * leads to an invalid system state.
 *
 */

public class BookKeeperStateBuilder implements StateBuilder {
    private static final Log LOG = LogFactory.getLog(BookKeeperStateBuilder.class);

    /*
     * Assuming that each entry is 1k bytes, we read 50k bytes at each call.
     * It provides a good degree of parallelism.
     */
    private static final long BK_READ_BATCH_SIZE = 50;
    private static final int PARALLEL_READS = 4;

    public static TSOState getState(TSOServerConfig config) {
        TSOState returnValue;
        if (!config.isRecoveryEnabled()) {
            LOG.warn("Logger is disabled");
            returnValue = new TSOState(new TimestampOracle());
            returnValue.initialize();
        } else {
            BookKeeperStateBuilder builder = new BookKeeperStateBuilder(config);

            try {
                returnValue = builder.buildState();
                LOG.info("State built");
            } catch (Throwable e) {
                LOG.error("Error while building the state.", e);
                returnValue = null;
            } finally {
                builder.shutdown();
            }
        }
        return returnValue;
    }

    private TimestampOracle timestampOracle;
    private ZooKeeper zk;
    private LoggerProtocol lp;
    private Semaphore throttleReads;
    private TSOServerConfig config;
    private BookKeeper bk;

    BookKeeperStateBuilder(TSOServerConfig config) {
        this.timestampOracle = new TimestampOracle();
        this.config = config;
        this.throttleReads = new Semaphore(PARALLEL_READS);
    }

    /**
     * Context objects for callbacks.
     *
     */
    class Context {
        TSOState state = null;
        TSOServerConfig config = null;
        boolean ready = false;
        boolean hasState = false;
        boolean hasLogger = false;
        int pending = 0;
        StateLogger logger;

        synchronized void setState(TSOState state) {
            this.state = state;
            hasState = true;
            validate();
        }

        synchronized void setLogger(StateLogger logger) {
            this.logger = logger;
            hasLogger = true;
            validate();
        }

        synchronized private void validate() {
            if (logger != null && state != null) {
                state.setLogger(logger);
            }

            if (hasLogger && hasState) {
                this.ready = true;
                notify();
            }
        }

        synchronized private void incrementPending() {
            pending++;
        }

        synchronized private void decrementPending() {
            pending--;
        }

        synchronized void abort() {
            this.ready = true;
            this.state = null;
            notify();
        }

        synchronized boolean isReady() {
            return ready;
        }

        synchronized boolean isFinished() {
            return ready && pending == 0;
        }
    }

    private class LoggerWatcher implements Watcher {
        CountDownLatch latch;

        LoggerWatcher(CountDownLatch latch) {
            this.latch = latch;
        }

        public void process(WatchedEvent event) {
            if (event.getState() != Watcher.Event.KeeperState.SyncConnected)
                shutdown();
            else
                latch.countDown();
        }
    }

    private class LockCreateCallback implements StringCallback {
        public void processResult(int rc, String path, Object ctx, String name) {
            if (rc != Code.OK) {
                LOG.warn("Failed to create lock znode: " + path);
                ((BookKeeperStateBuilder.Context) ctx).setState(null);
            } else {
                zk.getData(LoggerConstants.OMID_LEDGER_ID_PATH, false, new LedgerIdReadCallback(), ctx);
            }
        }
    }

    private class LedgerIdReadCallback implements DataCallback {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            if (rc == Code.OK) {
                buildStateFromLedger(data, ctx);
            } else if (rc == KeeperException.Code.NONODE.intValue()) {
                LOG.warn("No node exists. " + KeeperException.Code.get(rc).toString());
                TSOState tempState;
                try {
                    tempState = new TSOState(timestampOracle);
                } catch (Exception e) {
                    LOG.error("Error while creating state logger.", e);
                    tempState = null;
                }
                ((BookKeeperStateBuilder.Context) ctx).setState(tempState);
            } else {
                LOG.warn("Failed to read data. " + KeeperException.Code.get(rc).toString());
                ((BookKeeperStateBuilder.Context) ctx).setState(null);
            }
        }
    }

    /**
     * Invoked after the execution of a ledger read. Instances are
     * created in the open callback. 
     *
     */
    private class LoggerExecutor implements ReadCallback {
        public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> entries, Object ctx) {
            throttleReads.release();
            if (rc != BKException.Code.OK) {
                LOG.error("Error while reading ledger entries." + BKException.getMessage(rc));
                ((BookKeeperStateBuilder.Context) ctx).setState(null);
            } else {
                while (entries.hasMoreElements()) {
                    LedgerEntry le = entries.nextElement();
                    lp.execute(ByteBuffer.wrap(le.getEntry()));

                    if (lp.finishedRecovery() || le.getEntryId() == 0) {
                        ((BookKeeperStateBuilder.Context) ctx).setState(lp.getState());
                    }
                }
                ((BookKeeperStateBuilder.Context) ctx).decrementPending();
            }
        }
    }

    @Override
    public TSOState buildState() throws LoggerException {
        try {
            CountDownLatch latch = new CountDownLatch(1);

            this.zk = new ZooKeeper(config.getZkServers(), Integer.parseInt(System.getProperty("SESSIONTIMEOUT",
                    Integer.toString(10000))), new LoggerWatcher(latch));

            latch.await();
        } catch (Exception e) {
            LOG.error("Exception while starting zookeeper client", e);
            this.zk = null;
            throw LoggerException.create(Code.ZKOPFAILED);
        }

        LOG.info("Creating bookkeeper client");

        try {
            bk = new BookKeeper(new ClientConfiguration(), this.zk);
        } catch (Exception e) {
            LOG.error("Error while creating bookkeeper object", e);
            return null;
        }

        /*
         * Create ZooKeeper lock
         */

        Context ctx = new Context();
        ctx.config = this.config;

        zk.create(LoggerConstants.OMID_LOCK_PATH, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                new LockCreateCallback(), ctx);

        new BookKeeperStateLogger(zk).initialize(new LoggerInitCallback() {
            public void loggerInitComplete(int rc, StateLogger sl, Object ctx) {
                if (rc == Code.OK) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Logger is ok.");
                    }
                    ((Context) ctx).setLogger(sl);
                } else {
                    LOG.error("Error when initializing logger: " + LoggerException.getMessage(rc));
                }
            }

        }, ctx);

        try {
            synchronized (ctx) {
                if (!ctx.isFinished()) {
                    // TODO make configurable maximum waiting
                    ctx.wait();
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for state to build up.", e);
            ctx.setState(null);
        }

        return ctx.state;
    }

    /**
     * Disables this builder.    
     */
    @Override
    public void shutdown() {
        try {
            this.zk.close();
        } catch (InterruptedException e) {
            LOG.error("Error while shutting down", e);
        }
    }

    /**
     * Builds state from a ledger.
     * 
     * 
     * @param data
     * @param ctx
     * @return
     */
    private void buildStateFromLedger(byte[] data, Object ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Building state from ledger");
        }

        if (data == null) {
            LOG.error("No data on znode, can't determine ledger id");
            ((BookKeeperStateBuilder.Context) ctx).setState(null);
        }

        /*
         * Instantiates LoggerProtocol        
         */
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating logger protocol object");
        }
        try {
            this.lp = new LoggerProtocol(timestampOracle);
        } catch (Exception e) {
            LOG.error("Error while creating state logger for logger protocol.", e);
            ((BookKeeperStateBuilder.Context) ctx).setState(null);
        }

        /*
         * Open ledger for reading.
         */

        ByteBuffer bb = ByteBuffer.wrap(data);
        long ledgerId = bb.getLong();

        bk.asyncOpenLedger(ledgerId, BookKeeper.DigestType.CRC32, "flavio was here".getBytes(), new OpenCallback() {
            public void openComplete(int rc, LedgerHandle lh, Object ctx) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Open complete, ledger id: " + lh.getId());
                }
                if (rc != BKException.Code.OK) {
                    LOG.error("Could not open ledger for reading." + BKException.getMessage(rc));
                    ((BookKeeperStateBuilder.Context) ctx).setState(null);
                } else {
                    long counter = lh.getLastAddConfirmed();
                    while (counter >= 0) {
                        try {
                            throttleReads.acquire();
                        } catch (InterruptedException e) {
                            LOG.error("Couldn't build state", e);
                            ((Context) ctx).abort();
                            break;
                        }
                        if (((Context) ctx).isReady())
                            break;
                        ((Context) ctx).incrementPending();
                        long nextBatch = Math.max(counter - BK_READ_BATCH_SIZE + 1, 0);
                        lh.asyncReadEntries(nextBatch, counter, new LoggerExecutor(), ctx);
                        counter -= BK_READ_BATCH_SIZE;
                    }
                }
            }
        }, ctx);

    }

}
