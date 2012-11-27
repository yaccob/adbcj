package org.adbcj.h2;

import org.adbcj.DbException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class H2DbException extends DbException {
    private final String sqlstate;
    private final String message;
    private final String sql;
    private final int errorCode;
    private final String stackTrace;

    public H2DbException(String sqlstate, String message, String sql, int errorCode, String stackTrace) {
        super(message);
        this.sqlstate = sqlstate;
        this.message = message;
        this.sql = sql;
        this.errorCode = errorCode;
        this.stackTrace = stackTrace;
    }
}
