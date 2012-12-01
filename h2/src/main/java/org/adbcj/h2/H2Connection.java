package org.adbcj.h2;

import org.adbcj.*;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.DefaultDbSessionFuture;
import org.adbcj.support.DefaultResultEventsHandler;
import org.adbcj.support.DefaultResultSet;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author roman.stoffel@gamlor.info
 */
public class H2Connection implements Connection {
    private static final Logger logger = LoggerFactory.getLogger(H2Connection.class);
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
            int sessionId = nextId();
            final Request request = Request.createQuery(sql, eventHandler, accumulator, resultFuture, sessionId);
            queResponseHandlerAndSendMessage(request);
            return resultFuture;
        }
    }

    @Override
    public DbSessionFuture<Result> executeUpdate(String sql) {
        DefaultDbSessionFuture<Result> resultFuture = new DefaultDbSessionFuture<Result>(this);
        int sessionId = nextId();
        final Request request = Request.executeUpdate(sql, resultFuture, sessionId);
        queResponseHandlerAndSendMessage(request);
        return resultFuture;
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
            queResponseHandlerAndSendMessage(Request.createCloseRequest(closeFuture,this));
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

    public void queResponseHandlerAndSendMessage(Request request) {
        synchronized (lock){
            requestQueue.add(request);
            channel.write(request.getRequest());
        }
    }

    public String getSessionId() {
        return sessionId;
    }

    public Request dequeRequest() {
        synchronized (lock){
            final Request request = requestQueue.poll();
            if(logger.isDebugEnabled()){
                logger.debug("Dequeued request: {}",request);
            }
            return request;
        }
    }
    public int nextId() {
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
