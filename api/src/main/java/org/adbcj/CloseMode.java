package org.adbcj;

/**
 * @author roman.stoffel@gamlor.info
 */
public enum CloseMode {
    /**
     * Closes when pending operations have completed. All enqueued requests will be finished, before the resource is closed.
     *
     * However, no new requests / commands will be accepted.
     */
    CLOSE_GRACEFULLY,
    /**
     * Cancels all pending requests and the closes the resource.
     * No new requests / commands will be accepted.
     */
    CANCEL_PENDING_OPERATIONS;
}
