package org.adbcj.connectionpool;

import org.adbcj.*;
import org.adbcj.support.AbstractConnectionManager;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.FutureUtils;
import org.adbcj.support.OneArgFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author roman.stoffel@gamlor.info
 */
public class PooledConnectionManager extends AbstractConnectionManager implements PooledResource {
    private final Logger logger = LoggerFactory.getLogger(PooledConnectionManager.class);

    private final ConnectionManager connectionManager;
    private final ConcurrentLinkedQueue<Connection> poolOfConnections = new ConcurrentLinkedQueue<Connection>();
    private final ConcurrentLinkedQueue<DefaultDbFuture<Connection>> waitingForConnection
            = new ConcurrentLinkedQueue<DefaultDbFuture<Connection>>();
    private final ConcurrentHashMap<PooledConnection,Boolean> aliveConnections = new ConcurrentHashMap<PooledConnection,Boolean>();
    private final ConfigInfo config;

    private final Timer timeOutTimer = new Timer("PooledConnectionManager timeout timer",true);

    private final AtomicInteger allocatedConnectionsCount = new AtomicInteger();
    public PooledConnectionManager(ConnectionManager connectionManager,Map<String,String> properties, ConfigInfo config) {
        super(properties);
        this.connectionManager = connectionManager;
        this.config = config;
    }

    @Override
    public DbFuture<Connection> connect() {
        if(isClosed()){
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
        final DefaultDbFuture<Connection> connectionWaiter =new DefaultDbFuture<Connection>();
        waitingForConnection.offer(connectionWaiter);
        logger.info("Couldn't serve a connection, because the pool.maxPool limit of {} has been reached",config.getMaxConnections());
        timeOutTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                connectionWaiter.trySetException(new DbException("No connection available. Time out waiting for a connection. " +
                        "The "+ConfigInfo.MAX_WAIT_FOR_CONNECTIONS+" is set to "+config.getMaxWaitForConnectionsInMillisec() + ". " +
                        "The "+ConfigInfo.POOL_MAX_CONNECTIONS+" is set to "+config.getMaxConnections() + ". " ));
            }
        }, config.getMaxWaitForConnectionsInMillisec());
        return connectionWaiter;
    }

    @Override
    public DbFuture<Void> doClose(CloseMode mode) throws DbException {
        timeOutTimer.cancel();
        for (Connection pooledConnection : aliveConnections.keySet()) {
            pooledConnection.close(mode);
        }
        return connectionManager.close(mode);
    }

    public DbFuture<Void> returnConnection(PooledConnection pooledConnection) {
        final DefaultDbFuture<Void> transactionReturned = new DefaultDbFuture<Void>();
        aliveConnections.remove(pooledConnection);
        if(pooledConnection.isMayBeCorrupted()){
            allocatedConnectionsCount.decrementAndGet();
            return pooledConnection.getNativeConnection().close();
        } else {
            return returnConnectionToPool(pooledConnection, transactionReturned);
        }
    }

    private DbFuture<Void> returnConnectionToPool(PooledConnection pooledConnection, final DefaultDbFuture<Void> transactionReturned) {
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
        if(!tryCompleteWaitingConnectionRequests(nativeTx)){
            poolOfConnections.offer(nativeTx);
        }
        transactionReturned.setResult(null);
    }

    private boolean tryCompleteWaitingConnectionRequests(Connection nativeTx){
        DefaultDbFuture<Connection> waitForConnection = waitingForConnection.poll();
        while(null!=waitForConnection ){
            if(waitForConnection.trySetResult(nativeTx)){
                return true;
            }
            waitForConnection = waitingForConnection.poll();
        }
        return false;

    }


}
