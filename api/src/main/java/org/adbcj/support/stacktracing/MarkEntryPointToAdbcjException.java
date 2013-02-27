package org.adbcj.support.stacktracing;

/**
 * This excpetion is only for marking the entry point into ADBCJ.
 * @author roman.stoffel@gamlor.info
 */
public final class MarkEntryPointToAdbcjException extends Exception {
    public MarkEntryPointToAdbcjException() {
        super("The operation which caused this issue started in this stack-frame");
    }
}
