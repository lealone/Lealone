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

package com.yahoo.omid.tso.persistence;

/**
 * BookKeeper implementation of StateLogger.
 */

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.yahoo.omid.tso.TSOServerConfig;
import com.yahoo.omid.tso.persistence.StateLogger;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.LoggerInitCallback;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.AddRecordCallback;
import com.yahoo.omid.tso.persistence.LoggerConstants;
import com.yahoo.omid.tso.persistence.LoggerException.Code;

class BookKeeperStateLogger implements StateLogger {
    private static final Log LOG = LogFactory.getLog(BookKeeperStateLogger.class);

    private ZooKeeper zk;
    private BookKeeper bk;
    private LedgerHandle lh;

    /**
     * We try to acquire a lock for this primary first. If we succeed, then we check
     * if there is a ledger to recover from. 
     * 
     * The next two classes implement asynchronously the sequence of 
     * operations to write the ledger id. 
     */

    class LedgerIdCreateCallback implements StringCallback {
        LoggerInitCallback cb;
        byte[] ledgerId;

        LedgerIdCreateCallback(LoggerInitCallback cb, byte[] ledgerId) {
            this.cb = cb;
            this.ledgerId = ledgerId;
        }

        public void processResult(int rc, String path, Object ctx, String name) {
            if (rc == KeeperException.Code.OK.intValue()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Created znode succesfully: " + name);
                }

                BookKeeperStateLogger.this.enabled = true;
                cb.loggerInitComplete(Code.OK, BookKeeperStateLogger.this, ctx);
            } else if (rc != KeeperException.Code.NODEEXISTS.intValue()) {
                LOG.warn("Node exists: " + name);
                cb.loggerInitComplete(Code.INITLOCKFAILED, BookKeeperStateLogger.this, ctx);
            } else {
                zk.setData(LoggerConstants.OMID_LEDGER_ID_PATH, ledgerId, -1, new LedgerIdSetCallback(cb), ctx);
            }
        }
    }

    class LedgerIdSetCallback implements StatCallback {
        LoggerInitCallback cb;

        LedgerIdSetCallback(LoggerInitCallback cb) {
            this.cb = cb;
        }

        public void processResult(int rc, String path, Object ctx, Stat stat) {
            if (rc == KeeperException.Code.OK.intValue()) {
                LOG.debug("Set ledger id");
                BookKeeperStateLogger.this.enabled = true;
                cb.loggerInitComplete(Code.OK, BookKeeperStateLogger.this, ctx);
            } else {
                cb.loggerInitComplete(Code.ZKOPFAILED, BookKeeperStateLogger.this, ctx);
            }
        }

    }

    /**
     * Flag to determine whether this logger is operating or not.
     */
    boolean enabled = false;

    /**
     * Constructor creates a zookeeper and a bookkeeper objects.
     */
    BookKeeperStateLogger(ZooKeeper zk) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Constructing Logger");
        }

        this.zk = zk;
    }

    /**
     * Watcher for the zookeeper object.
     */

    class LoggerWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if (event.getState() != Watcher.Event.KeeperState.SyncConnected)
                shutdown();
        }
    }

    /**
     * Initializes this logger object to add records. Implements the initialize 
     * method of the StateLogger interface.
     * 
     * @param cb
     * @param ctx
     */

    @Override
    public void initialize(final LoggerInitCallback cb, Object ctx) throws LoggerException {
        TSOServerConfig config = ((BookKeeperStateBuilder.Context) ctx).config;

        /*
         * Create new ledger for adding records
         */
        try {
            bk = new BookKeeper(new ClientConfiguration(), zk);
        } catch (Exception e) {
            LOG.error("Exception while initializing bookkeeper", e);
            throw new LoggerException.BKOpFailedException();
        }

        bk.asyncCreateLedger(config.getEnsembleSize(), config.getQuorumSize(), BookKeeper.DigestType.CRC32,
                "flavio was here".getBytes(), new CreateCallback() {
                    @Override
                    public void createComplete(int rc, LedgerHandle lh, Object ctx) {
                        if (rc == BKException.Code.OK) {
                            try {
                                BookKeeperStateLogger.this.lh = lh;

                                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                                DataOutputStream dos = new DataOutputStream(bos);
                                dos.writeLong(lh.getId());

                                zk.create(LoggerConstants.OMID_LEDGER_ID_PATH, bos.toByteArray(), Ids.OPEN_ACL_UNSAFE,
                                        CreateMode.PERSISTENT, new LedgerIdCreateCallback(cb, bos.toByteArray()), ctx);
                            } catch (IOException e) {
                                LOG.error("Failed to write to zookeeper. ", e);
                                cb.loggerInitComplete(Code.BKOPFAILED, BookKeeperStateLogger.this, ctx);
                            }
                        } else {
                            LOG.error("Failed to create ledger. " + BKException.getMessage(rc));
                            cb.loggerInitComplete(Code.BKOPFAILED, BookKeeperStateLogger.this, ctx);
                        }
                    }
                }, ctx);
    }

    /**
     * Adds a record to the log of operations. The record is a byte array.
     * 
     * @param record
     * @param cb
     * @param ctx
     */
    @Override
    public void addRecord(byte[] record, final AddRecordCallback cb, Object ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding record.");
        }

        if (!enabled) {
            cb.addRecordComplete(Code.LOGGERDISABLED, ctx);
            return;
        }

        this.lh.asyncAddEntry(record, new AddCallback() {
            @Override
            public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                if (LOG.isDebugEnabled()) {
                    LOG.info("Add to ledger complete: " + lh.getId() + ", " + entryId);
                }
                if (rc != BKException.Code.OK) {
                    LOG.error("Asynchronous add entry failed: " + BKException.getMessage(rc));
                    cb.addRecordComplete(Code.ADDFAILED, ctx);
                } else {
                    cb.addRecordComplete(Code.OK, ctx);
                }
            }
        }, ctx);
    }

    /**
     * Shuts down this logger.
     * 
     */
    public void shutdown() {
        enabled = false;
        try {
            try {
                if (zk.getState() == ZooKeeper.States.CONNECTED) {
                    zk.delete(LoggerConstants.OMID_LEDGER_ID_PATH, -1);
                }
            } catch (Exception e) {
                LOG.warn("Exception while deleting lock znode", e);
            }
            if (this.bk != null)
                bk.close();
            if (this.zk != null)
                zk.close();
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while closing logger.", e);
        } catch (BKException e) {
            LOG.warn("Exception while closing BookKeeper object.", e);
        }
    }

}
