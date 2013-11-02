package com.yahoo.ycsb;

public interface TransactionalDB {
    public int getNextTransactionLength();
    public int beginTransaction();
    public int endTransaction();
}
