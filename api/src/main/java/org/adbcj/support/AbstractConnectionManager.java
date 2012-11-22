package org.adbcj.support;

import org.adbcj.*;

import java.util.Collections;
import java.util.Map;

/**
 * Abstract implementation of a {@link ConnectionManager}. It does following things for you:
 *
 * @author roman.stoffel@gamlor.info
 */
public abstract class AbstractConnectionManager implements ConnectionManager {


    protected final Map<String, String> properties;
    private volatile DbFuture<Void> closeFuture = null;

    public AbstractConnectionManager(Map<String, String> properties) {
        this.properties = Collections.unmodifiableMap(properties);
    }

    public DbFuture<Void> close() {
        return close(CloseMode.CLOSE_GRACEFULLY);
    }

    @Override
    public final DbFuture<Void> close(CloseMode mode) throws DbException {
        if (!isClosed()) {
            synchronized (this) {
                if (!isClosed()) {
                    closeFuture = doClose(mode);
                }
            }
        }
        return closeFuture;
    }

    protected abstract DbFuture<Void> doClose(CloseMode mode);

    public int maxConnections() {
        try {
            int maxConnections = Integer.parseInt(properties.get(StandardProperties.MAX_QUEUE_LENGTH));
            if (maxConnections <= 0) {
                throw new IllegalArgumentException("The property " + StandardProperties.MAX_QUEUE_LENGTH + " has to be positive number");
            }
            return maxConnections;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("The property " + StandardProperties.MAX_QUEUE_LENGTH + " has to be positive number");
        }
    }


    public final boolean isClosed() {
        return closeFuture != null;
    }
}
