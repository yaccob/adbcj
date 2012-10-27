package org.adbcj.connectionpool;

import org.adbcj.*;
import org.adbcj.support.AbstractConnectionManager;

/**
 * @author roman.stoffel@gamlor.info
 */
public class PooledConnectionManager extends AbstractConnectionManager {
    private final ConnectionManager connectionManager;

    public PooledConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public DbFuture<Connection> connect() {
        return connectionManager.connect();
    }

    @Override
    public DbFuture<Void> close(CloseMode mode) throws DbException {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public boolean isClosed() {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }
}
