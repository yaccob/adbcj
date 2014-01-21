package org.adbcj.connectionpool;

import org.adbcj.*;
import org.adbcj.support.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class UsersConnectionPool extends AbstractConnectionManager implements PooledResource {

    private final Logger logger = LoggerFactory.getLogger(UsersConnectionPool.class);

    private final ConnectionManager connectionManager;
    private final ConcurrentLinkedQueue<ConnectionItem> poolOfConnections = new ConcurrentLinkedQueue<ConnectionItem>();
    private final ConcurrentLinkedQueue<DefaultDbFuture<ConnectionItem>> waitingForConnection
            = new ConcurrentLinkedQueue<DefaultDbFuture<ConnectionItem>>();
    private final ConcurrentHashMap<PooledConnection,Boolean> aliveConnections = new ConcurrentHashMap<PooledConnection,Boolean>();
    private final ConfigInfo config;

    private final AtomicInteger allocatedConnectionsCount = new AtomicInteger();

    private final Timer timeOutTimer;
    private final UserConnectionId forUser;

    public UsersConnectionPool(ConnectionManager connectionManager,
                               Timer timeOutTimer,
                               UserConnectionId forUser,
                               Map<String,String> properties,
                               ConfigInfo config) {
        super(properties);
        this.timeOutTimer = timeOutTimer;
        this.forUser = forUser;
        this.connectionManager = connectionManager;
        this.config = config;
    }

    @Override
    public DbFuture<Connection> connect() {
        if(isClosed()){
            throw new DbException("Connection manager is closed. Cannot open a new connection");
        }
        if(!this.forUser.isDefault){
            throw new IllegalStateException("Can only connect with default user");
        }
        return connectImpl(null,null);
    }

    @Override
    public DbFuture<Connection> connect(String user, String password) {
        if(isClosed()){
            throw new DbException("Connection manager is closed. Cannot open a new connection");
        }
        if(!this.forUser.userId.equals(user)){
            throw new DbException("Needs to connet with user " +this.forUser +" but tried connecting with "+user );
        }
        return connectImpl(user,password);
    }

    private DbFuture<ConnectionItem> findOrGetNewConnection(String userName,String pwd) {
        ConnectionItem connection = poolOfConnections.poll();
        if(null!=connection){
            return DefaultDbFuture.completed(connection);
        }
        if(allocatedConnectionsCount.get()>=config.getMaxConnections()){
            return waitForConnection();
        }  else{
            allocatedConnectionsCount.incrementAndGet();
            return FutureUtils.map(delegateConnect(userName,pwd), new OneArgFunction<Connection, ConnectionItem>() {
                @Override
                public ConnectionItem apply(Connection arg) {
                    return new ConnectionItem(arg, config.getStatementCacheSize());
                }
            });
        }
    }

    private DbFuture<Connection> delegateConnect(String userName,String pwd) {
        if(forUser.isDefault){
            return connectionManager.connect();
        } else{
            return connectionManager.connect(userName,pwd);

        }
    }

    private DbFuture<ConnectionItem> waitForConnection() {
        final DefaultDbFuture<ConnectionItem> connectionWaiter =new DefaultDbFuture<ConnectionItem>(stackTracingOptions());
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
        final DefaultDbFuture<Void> transactionReturned = new DefaultDbFuture<Void>(stackTracingOptions());
        aliveConnections.remove(pooledConnection);
        if(pooledConnection.isMayBeCorrupted()){
            allocatedConnectionsCount.decrementAndGet();
            return pooledConnection.getNativeConnection().close();
        } else {
            return returnConnectionToPool(pooledConnection, transactionReturned);
        }
    }

    private DbFuture<Connection> connectImpl(String userName,String pwd) {
        return (DbFuture) FutureUtils.map(findOrGetNewConnection(userName,pwd), new OneArgFunction<ConnectionItem, PooledConnection>() {
            @Override
            public PooledConnection apply(ConnectionItem arg) {
                final PooledConnection pooledConnection = new PooledConnection(
                        arg,
                        UsersConnectionPool.this);
                aliveConnections.put(pooledConnection, true);
                return pooledConnection;
            }
        });
    }

    private DbFuture<Void> returnConnectionToPool(PooledConnection pooledConnection, final DefaultDbFuture<Void> transactionReturned) {
        final ConnectionItem item = pooledConnection.connectionItem();
        final Connection nativeTx = item.connection();
        if(!nativeTx.isClosed() && nativeTx.isInTransaction()){
            nativeTx.rollback().addListener(new DbListener<Void>() {
                @Override
                public void onCompletion(DbFuture<Void> future) {
                    UsersConnectionPool.this.returnConnection(item, transactionReturned);
                }
            });
        } else {
            UsersConnectionPool.this.returnConnection(item, transactionReturned);
        }
        return transactionReturned;
    }

    private void returnConnection(ConnectionItem nativeTx, DefaultDbFuture<Void> transactionReturned) {
        if(!tryCompleteWaitingConnectionRequests(nativeTx)){
            poolOfConnections.offer(nativeTx);
        }
        transactionReturned.setResult(null);
    }

    private boolean tryCompleteWaitingConnectionRequests(ConnectionItem nativeTx){
        DefaultDbFuture<ConnectionItem> waitForConnection = waitingForConnection.poll();
        while(null!=waitForConnection ){
            if(waitForConnection.trySetResult(nativeTx)){
                return true;
            }
            waitForConnection = waitingForConnection.poll();
        }
        return false;

    }

   public static class UserConnectionId{
       public final String userId;
       public final boolean isDefault;
       public UserConnectionId(String userId, boolean isDefault) {
           this.userId = userId;
           this.isDefault = isDefault;
       }

       @Override
       public boolean equals(Object o) {
           if (this == o) return true;
           if (o == null || getClass() != o.getClass()) return false;

           UserConnectionId that = (UserConnectionId) o;

           if (userId != null ? !userId.equalsIgnoreCase(that.userId) : that.userId != null) return false;

           return true;
       }

       @Override
       public int hashCode() {
           return userId != null ? userId.hashCode() : 0;
       }
   }
}
