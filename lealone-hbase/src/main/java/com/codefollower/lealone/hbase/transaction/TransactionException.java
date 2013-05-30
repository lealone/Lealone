package com.codefollower.lealone.hbase.transaction;

public class TransactionException extends Exception {

    private static final long serialVersionUID = 1L;

    public TransactionException(String reason) {
        super(reason);
    }

    public TransactionException(String reason, Exception e) {
        super(reason, e);
    }
}
