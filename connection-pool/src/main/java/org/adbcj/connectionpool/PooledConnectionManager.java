package org.adbcj.connectionpool;

import org.adbcj.*;
import org.adbcj.support.AbstractConnectionManager;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.FutureUtils;
import org.adbcj.support.OneArgFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author roman.stoffel@gamlor.info
 */
public class PooledConnectionManager extends AbstractConnectionManager implements PooledResource {
    private final ConnectionManager connectionManager;
    private final ConcurrentLinkedQueue<Connection> poolOfConnections = new ConcurrentLinkedQueue<Connection>();
    private final ConcurrentLinkedQueue<DefaultDbFuture<Connection>> waitingForConnection
            = new ConcurrentLinkedQueue<DefaultDbFuture<Connection>>();
    private final ConcurrentHashMap<PooledConnection,Boolean> aliveConnections = new ConcurrentHashMap<PooledConnection,Boolean>();
    private volatile boolean closed;
    private final ConfigInfo config;

    private final AtomicInteger allocatedConnectionsCount = new AtomicInteger();
    public PooledConnectionManager(ConnectionManager connectionManager, ConfigInfo config) {
        this.connectionManager = connectionManager;
        this.config = config;
    }

    @Override
    public DbFuture<Connection> connect() {
        if(closed){
            throw new DbException("Connection manager is closed. Cannot open a new connection");
        }
        return (DbFuture) FutureUtils.map(findOrGetNewConnection(), new OneArgFunction<Connection, PooledConnection>() {
            @Override
            public PooledConnection apply(Connection arg) {
                final PooledConnection pooledConnection = new PooledConnection(arg, PooledConnectionManager.this);
                aliveConnections.put(pooledConnection, true);
                return pooledConnection;
            }
        });
    }

    private DbFuture<Connection> findOrGetNewConnection() {
        Connection connection = poolOfConnections.poll();
        if(null!=connection){
            return DefaultDbFuture.completed(connection);
        }
        if(allocatedConnectionsCount.get()>=config.getMaxConnections()){
            return waitForConnection();
        }  else{
            allocatedConnectionsCount.incrementAndGet();
            return connectionManager.connect();
        }
    }

    private DbFuture<Connection> waitForConnection() {
        DefaultDbFuture<Connection> connection = new DefaultDbFuture<Connection>();
        waitingForConnection.offer(connection);
        return connection;
    }

    @Override
    public DbFuture<Void> close(CloseMode mode) throws DbException {
        closed = true;
        for (Connection pooledConnection : aliveConnections.keySet()) {
            pooledConnection.close(mode);
        }
        return connectionManager.close(mode);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    public DbFuture<Void> returnConnection(PooledConnection pooledConnection) {
        final DefaultDbFuture<Void> transactionReturned = new DefaultDbFuture<Void>();
        aliveConnections.remove(pooledConnection);
        final Connection nativeTx = pooledConnection.getNativeConnection();
        if(!nativeTx.isClosed() && nativeTx.isInTransaction()){
            nativeTx.rollback().addListener(new DbListener<Void>() {
                @Override
                public void onCompletion(DbFuture<Void> future) {
                    PooledConnectionManager.this.returnConnection(nativeTx, transactionReturned);
                }
            });
        } else {
            PooledConnectionManager.this.returnConnection(nativeTx, transactionReturned);
        }
        return transactionReturned;
    }

    private void returnConnection(Connection nativeTx, DefaultDbFuture<Void> transactionReturned) {
        final DefaultDbFuture<Connection> waitForConnection = waitingForConnection.poll();
        if(null!=waitForConnection){
            waitForConnection.setResult(nativeTx);
        } else{
            poolOfConnections.offer(nativeTx);
        }
        transactionReturned.setResult(null);
    }
}
