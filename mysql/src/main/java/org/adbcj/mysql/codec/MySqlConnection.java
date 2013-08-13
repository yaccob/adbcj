package org.adbcj.mysql.codec;

import org.adbcj.*;
import org.adbcj.mysql.netty.MysqlConnectionManager;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.DefaultResultEventsHandler;
import org.adbcj.support.DefaultResultSet;
import io.netty.channel.Channel;
import org.adbcj.support.stacktracing.StackTracingOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.EnumSet;
import java.util.Set;

public class MySqlConnection implements Connection {

    private static final Logger logger = LoggerFactory.getLogger(MySqlConnection.class);

    private final int maxQueueSize;
    private final MysqlConnectionManager connectionManager;
    private final Channel channel;

    protected final int id;

    private final ArrayDeque<MySqlRequest> requestQueue;

    private final Object lock = new Object();
    private volatile DefaultDbFuture<Void> closeFuture;
    private volatile boolean isInTransaction = false;

    public MySqlConnection(int maxQueueSize, MysqlConnectionManager connectionManager, Channel channel) {
        this.maxQueueSize = maxQueueSize;
        this.connectionManager = connectionManager;
        this.channel = channel;
        this.id = connectionManager.nextId();
        connectionManager.addConnection(this);

        synchronized (lock) {
            requestQueue = new ArrayDeque<MySqlRequest>(maxQueueSize + 1);
        }
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public synchronized DbFuture<Void> close() throws DbException {
        return close(CloseMode.CLOSE_GRACEFULLY);
    }


    @Override
    public void beginTransaction() {
        if(isInTransaction()){
            throw new DbException("This connection is already in a transaction");
        }
        checkClosed();
        synchronized (lock){
            forceQueRequest(MySqlRequests.beginTransaction(this));
            isInTransaction = true;
        }
    }

    @Override
    public DbFuture<Void> commit() {
        if(!isInTransaction()){
            throw new DbException("No transaction has been started to commit");
        }
        checkClosed();
        synchronized (lock){
            final MySqlRequest request = queRequest(MySqlRequests.commitTransaction(this));
            isInTransaction = false;
            return (DbFuture<Void>) request.getFuture();
        }
    }

    @Override
    public DbFuture<Void> rollback() {
        if(!isInTransaction()){
            throw new DbException("No transaction has been started to rollback");
        }
        checkClosed();
        synchronized (lock){
            final MySqlRequest request = queRequest(MySqlRequests.rollbackTransaction(this));
            isInTransaction = false;
            return (DbFuture<Void>) request.getFuture();
        }
    }

    @Override
    public boolean isInTransaction() {
        return isInTransaction;
    }

    @Override
    public DbFuture<ResultSet> executeQuery(String sql) {
        checkClosed();
        DefaultResultSet rs = new DefaultResultSet();
        DefaultResultEventsHandler handler = new DefaultResultEventsHandler();
        return (DbFuture) executeQuery(sql, handler, rs);
    }

    @Override
    public <T> DbFuture<T> executeQuery(String sql, ResultHandler<T> eventHandler, T accumulator) {
        checkClosed();
        return (DbFuture) queRequest(MySqlRequests.executeQuery(sql,
                eventHandler,
                accumulator,
                this)).getFuture();
    }

    @Override
    public DbFuture<Result> executeUpdate(String sql) {
        checkClosed();
        return (DbFuture) queRequest(MySqlRequests.executeUpdate(sql,
                this)).getFuture();
    }

    @Override
    public DbFuture<PreparedQuery> prepareQuery(String sql) {
        checkClosed();
        return (DbFuture) queRequest(MySqlRequests.prepareQuery(sql,
                this)).getFuture();
    }

    @Override
    public DbFuture<PreparedUpdate> prepareUpdate(String sql) {
        checkClosed();
        return (DbFuture) queRequest(MySqlRequests.prepareQuery(sql,
                this)).getFuture();
    }

    @Override
    public DbFuture<Void> close(CloseMode closeMode) throws DbException {
        synchronized (lock) {
            if (null == closeFuture) {
                final MySqlRequest closeRequest = MySqlRequests.createCloseRequest(this);
                closeFuture = closeRequest.getFuture();
                closeFuture.addListener(new DbListener<Void>() {
                    @Override
                    public void onCompletion(DbFuture<Void> future) {
                        MySqlConnection.this.connectionManager.removeConnection(MySqlConnection.this);
                    }
                });
                if(closeMode==CloseMode.CANCEL_PENDING_OPERATIONS){
                    forceCloseOnPendingRequests();
                }
                forceQueRequest(closeRequest);
                return closeFuture;
            } else {
                return closeFuture;
            }
        }
    }

    @Override
    public boolean isClosed() throws DbException {
        return closeFuture != null;
    }

    @Override
    public boolean isOpen() throws DbException {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }


    void checkClosed() {
        if(isClosed()){
            throw new DbSessionClosedException("This connection is closed");
        }
    }

    private static final Set<ClientCapabilities> CLIENT_CAPABILITIES = EnumSet.of(
            ClientCapabilities.LONG_PASSWORD,
            ClientCapabilities.FOUND_ROWS,
            ClientCapabilities.LONG_COLUMN_FLAG,
            ClientCapabilities.CONNECT_WITH_DB,
            ClientCapabilities.LOCAL_FILES,
            ClientCapabilities.PROTOCOL_4_1,
            ClientCapabilities.TRANSACTIONS,
            ClientCapabilities.SECURE_AUTHENTICATION);

    public Set<ClientCapabilities> getClientCapabilities() {
        return CLIENT_CAPABILITIES;
    }

    private static final Set<ExtendedClientCapabilities> EXTENDED_CLIENT_CAPABILITIES = EnumSet.of(
            ExtendedClientCapabilities.MULTI_RESULTS
    );

    public Set<ExtendedClientCapabilities> getExtendedClientCapabilities() {
        return EXTENDED_CLIENT_CAPABILITIES;
    }

    public MySqlRequest queRequest(MySqlRequest request) {
        synchronized (lock) {

            int requestsPending = requestQueue.size();
            if (requestsPending > maxQueueSize) {
                throw new DbException("To many pending requests. The current maximum is " + maxQueueSize + "." +
                        "Ensure that your not overloading the database with requests. " +
                        "Also check the " + StandardProperties.MAX_QUEUE_LENGTH + " property");
            }
            return forceQueRequest(request);
        }
    }

    public MySqlRequest forceQueRequest(MySqlRequest request) {
        synchronized (lock) {
            requestQueue.add(request);
            channel.writeAndFlush(request.getRequest());
            return request;
        }
    }

    public void tryCompleteClose() {
        synchronized (lock) {
            if (null != closeFuture) {
                closeFuture.trySetResult(null);
            }
        }
    }

    public MySqlRequest dequeRequest() {
            synchronized (lock){
                final MySqlRequest request = requestQueue.poll();
                if(logger.isDebugEnabled()){
                    logger.debug("Dequeued request: {}",request);
                }
                if(request.getRequest().wasCancelled()){
                    if(logger.isDebugEnabled()){
                        logger.debug("Request has been cancelled: {}",request);
                    }
                    return dequeRequest();
                }
                return request;
        }
    }

    public Object lock() {
        return lock;
    }

    public StackTracingOptions stackTraceOptions(){
        return this.connectionManager.stackTracingOptions();
    }


    private void forceCloseOnPendingRequests() {
        for (MySqlRequest request : requestQueue) {
            if(request.getRequest().tryCancel()){
                request.getFuture().trySetException(new DbSessionClosedException("Connection is closed"));
            }
        }
    }
}
