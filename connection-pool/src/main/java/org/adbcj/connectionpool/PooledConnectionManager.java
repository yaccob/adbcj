package org.adbcj.connectionpool;

import org.adbcj.*;
import org.adbcj.support.AbstractConnectionManager;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.OneArgFunction;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author roman.stoffel@gamlor.info
 */
public class PooledConnectionManager extends AbstractConnectionManager {
    private final ConnectionManager connectionManager;
    private final ConcurrentLinkedQueue<Connection> poolOfConnections = new ConcurrentLinkedQueue<Connection>();

    public PooledConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public DbFuture<Connection> connect() {
        return (DbFuture) findOrGetNewConnection().map(new OneArgFunction<Connection, PooledConnection>() {
            @Override
            public PooledConnection apply(Connection arg) {
                return new PooledConnection(arg, PooledConnectionManager.this);
            }
        });
    }

    private DbFuture<Connection> findOrGetNewConnection() {
        Connection connection = poolOfConnections.poll();
        if(null!=connection){
            return DefaultDbFuture.completed(connection);
        }
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

    public DbFuture<Void> returnConnection(PooledConnection pooledConnection) {
        poolOfConnections.offer(pooledConnection.getNativeConnection());
        return DefaultDbFuture.completed(null);
    }
}
