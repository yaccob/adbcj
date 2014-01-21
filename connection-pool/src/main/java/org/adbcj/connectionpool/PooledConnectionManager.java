package org.adbcj.connectionpool;

import org.adbcj.*;
import org.adbcj.support.AbstractConnectionManager;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.FutureUtils;
import org.adbcj.support.OneArgFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author roman.stoffel@gamlor.info
 */
public class PooledConnectionManager extends AbstractConnectionManager implements PooledResource {

    private final UsersConnectionPool.UserConnectionId defaultConnection = new UsersConnectionPool.UserConnectionId("",true);

    private final Timer timeOutTimer = new Timer("PooledConnectionManager timeout timer",true);

    private final ConcurrentHashMap<UsersConnectionPool.UserConnectionId,UsersConnectionPool> connectionsPerUser
            = new ConcurrentHashMap<UsersConnectionPool.UserConnectionId, UsersConnectionPool>();
    private final ConnectionManager connectionManager;
    private final ConfigInfo config;

    public PooledConnectionManager(ConnectionManager connectionManager,Map<String, String> properties,ConfigInfo config) {
        super(properties);
        this.connectionManager = connectionManager;
        this.config = config;
    }

    @Override
    protected DbFuture<Void> doClose(CloseMode mode) {
        final DefaultDbFuture<Void> toComplete = new DefaultDbFuture<Void>(stackTracingOptions());
        final ArrayList<UsersConnectionPool> toClose = new ArrayList<UsersConnectionPool>(connectionsPerUser.values());
        final AtomicInteger countDown = new AtomicInteger(toClose.size());
        final AtomicReference<Exception> failure = new AtomicReference(null);
        for(UsersConnectionPool pool : connectionsPerUser.values()){
            pool.close(mode).addListener(new DbListener<Void>() {
                @Override
                public void onCompletion(DbFuture<Void> future) {
                    if(future.getState()==FutureState.FAILURE){
                        failure.compareAndSet(null,future.getException());
                    }
                    int pendingCloses = countDown.decrementAndGet();
                    if(pendingCloses==0){
                        if(failure.get()==null){
                            toComplete.trySetResult(null);
                        } else{
                            toComplete.trySetException(failure.get());
                        }
                    }
                }
            });
        }
        return toComplete;
    }

    @Override
    public DbFuture<Connection> connect() {
        return connectByKey(defaultConnection).connect();
    }

    @Override
    public DbFuture<Connection> connect(String user, String password) {

        return connectByKey(new UsersConnectionPool.UserConnectionId(user,false)).connect(user, password);
    }

    private UsersConnectionPool connectByKey(UsersConnectionPool.UserConnectionId key) {
        UsersConnectionPool connectionPool = connectionsPerUser.get(key);
        if(null==connectionPool){
            connectionsPerUser.putIfAbsent(key,new UsersConnectionPool(connectionManager,timeOutTimer,key,properties, config));
            connectionPool = connectionsPerUser.get(key);
        }
        return connectionPool;
    }
}
