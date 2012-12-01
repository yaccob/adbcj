package org.adbcj.h2;

import org.adbcj.*;
import org.adbcj.h2.packets.CloseCommand;
import org.adbcj.h2.packets.DirectQuery;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.DefaultDbSessionFuture;
import org.adbcj.support.DefaultResultEventsHandler;
import org.adbcj.support.DefaultResultSet;
import org.jboss.netty.channel.Channel;

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author roman.stoffel@gamlor.info
 */
public class H2Connection implements Connection {
    private final String sessionId = StringUtils.convertBytesToHex(MathUtils.secureRandomBytes(32));
    private final ArrayDeque<Request> requestQueue;
    private final int maxQueueSize;
    private final ConnectionManager manager;
    private final Channel channel;
    private final Object lock = new Object();
    private volatile DefaultDbFuture<Void> closeFuture;
    private final AtomicInteger requestId = new AtomicInteger(0);

    public H2Connection(int maxQueueSize, ConnectionManager manager, Channel channel) {
        this.maxQueueSize = maxQueueSize;
        this.manager = manager;
        this.channel = channel;
        synchronized (lock){
            requestQueue = new ArrayDeque<Request>(maxQueueSize+1);
        }
    }


    @Override
    public ConnectionManager getConnectionManager() {
        return manager;
    }

    @Override
    public void beginTransaction() {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public DbSessionFuture<Void> commit() {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public DbSessionFuture<Void> rollback() {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public boolean isInTransaction() {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public DbSessionFuture<ResultSet> executeQuery(String sql) {
        ResultHandler<DefaultResultSet> eventHandler = new DefaultResultEventsHandler();
        DefaultResultSet resultSet = new DefaultResultSet();
        return (DbSessionFuture) executeQuery(sql, eventHandler, resultSet);
    }

    @Override
    public <T> DbSessionFuture<T> executeQuery(String sql, ResultHandler<T> eventHandler, T accumulator) {
        synchronized (lock){
            DefaultDbSessionFuture<T> resultFuture = new DefaultDbSessionFuture<T>(this);
            requestQueue.add(Request.createQuery(sql,eventHandler, accumulator,resultFuture));
            channel.write(new DirectQuery(nextId(),nextId(),sql));
            return resultFuture;
        }
    }

    @Override
    public DbSessionFuture<Result> executeUpdate(String sql) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public DbSessionFuture<PreparedQuery> prepareQuery(String sql) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public DbSessionFuture<PreparedUpdate> prepareUpdate(String sql) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public DbFuture<Void> close() throws DbException {
        return close(CloseMode.CLOSE_GRACEFULLY);
    }

    @Override
    public DbFuture<Void> close(CloseMode closeMode) throws DbException {
        synchronized (lock){
            if(this.closeFuture!=null){
                return closeFuture;
            }
            closeFuture = new DefaultDbFuture<Void>();
            requestQueue.add(Request.createCloseRequest(closeFuture,this));
            channel.write(new CloseCommand());
            return closeFuture;
        }
    }

    @Override
    public boolean isClosed() throws DbException {
        return null!=closeFuture;
    }

    @Override
    public boolean isOpen() throws DbException {
        return !isClosed();
    }

    public String getSessionId() {
        return sessionId;
    }

    public Request dequeRequest() {
        synchronized (lock){
            final Request request = requestQueue.poll();
            return request;
        }
    }
    private int nextId() {
        return requestId.incrementAndGet();
    }

    public void tryCompleteClose() {
        synchronized (lock){
            if(null!=closeFuture){
                closeFuture.trySetResult(null);
            }
        }
    }
}
