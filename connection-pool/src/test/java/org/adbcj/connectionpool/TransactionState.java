package org.adbcj.connectionpool;

/**
 * @author roman.stoffel@gamlor.info
 */
public enum TransactionState {
    NONE,
    ACTIVE,
    ROLLED_BACK
}
