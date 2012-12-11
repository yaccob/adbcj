package org.adbcj.h2.protocol;

import org.adbcj.DbException;

/**
 * @author roman.stoffel@gamlor.info
 */
public enum StatusCodes {
    STATUS_ERROR(0),
    STATUS_OK(1);

    private final int statusValue;

    private StatusCodes(int statusValue) {
        this.statusValue = statusValue;
    }

    public int getStatusValue() {
        return statusValue;
    }

    public boolean isStatus(int status) {
        return this.statusValue == status;
    }

    /**
     * Expect this status or throw
     * @param status
     */
    public void expectStatusOrThrow(int status) {
        if(!isStatus(status)){
            throw new DbException("Expected status: "+this+" bus got: "+status);
        }
    }
}
