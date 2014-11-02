/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.engine;

/**
 * Represents an in-doubt transaction (a transaction in the prepare phase).
 */
public interface InDoubtTransaction {

    /**
     * The transaction state meaning this transaction is not committed yet, but
     * also not rolled back (in-doubt).
     */
    public static final int IN_DOUBT = 0;

    /**
     * The transaction state meaning this transaction is committed.
     */
    public static final int COMMIT = 1;

    /**
     * The transaction state meaning this transaction is rolled back.
     */
    public static final int ROLLBACK = 2;

    /**
     * Change the state of this transaction.
     * This will also update the transaction log.
     *
     * @param state the new state
     */
    public void setState(int state);

    /**
     * Get the state of this transaction as a text.
     *
     * @return the transaction state text
     */
    public String getState();

    /**
     * Get the name of the transaction.
     *
     * @return the transaction name
     */
    public String getTransaction();

}
