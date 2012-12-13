package org.adbcj.h2;

import org.adbcj.*;
import org.adbcj.support.*;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
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
    private final int autoIdSession = nextId();
    private final int commitIdSession = nextId();
    private final int rollbackIdSession = nextId();
    private BlockingRequestInProgress blockingRequest;

    private volatile boolean isInTransaction = false;
    private DbSessionFuture<PreparedUpdate> commit = null;
    private DbSessionFuture<PreparedUpdate> rollback;

    private final RequestCreator requestCreator = new RequestCreator(this);

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
        checkClosed();
        synchronized (lock){
            if (isInTransaction()) {
                throw new DbException("Cannot begin new transaction.  Current transaction needs to be committed or rolled back");
            }
            isInTransaction = true;
            final Request request = requestCreator.beginTransaction();
            queRequest(request);
        }
    }

    @Override
    public DbSessionFuture<Void> commit() {
        checkClosed();
        synchronized (lock){
            if (!isInTransaction()) {
                throw new DbException("Not currently in a transaction, cannot commit");
            }
            final Request request = requestCreator.commitTransaction();
            queRequest(request);
            isInTransaction = false;
            return (DbSessionFuture<Void>) request.getToComplete();
        }
    }

    @Override
    public DbSessionFuture<Void> rollback() {
        checkClosed();
        synchronized (lock){
            if (!isInTransaction()) {
                throw new DbException("Not currently in a transaction, cannot rollback");
            }
            final Request request = requestCreator.rollbackTransaction();
            queRequest(request);
            isInTransaction = false;
            return (DbSessionFuture<Void>) request.getToComplete();
        }
    }

    @Override
    public boolean isInTransaction() {
        return isInTransaction;
    }

    @Override
    public DbSessionFuture<ResultSet> executeQuery(String sql) {
        checkClosed();
        ResultHandler<DefaultResultSet> eventHandler = new DefaultResultEventsHandler();
        DefaultResultSet resultSet = new DefaultResultSet();
        return (DbSessionFuture) executeQuery(sql, eventHandler, resultSet);
    }

    @Override
    public <T> DbSessionFuture<T> executeQuery(String sql, ResultHandler<T> eventHandler, T accumulator) {
        checkClosed();
        synchronized (lock){
            final Request request = requestCreator.createQuery(sql, eventHandler, accumulator);
            queRequest(request);
            return (DbSessionFuture<T>) request.getToComplete();
        }
    }

    @Override
    public DbSessionFuture<Result> executeUpdate(String sql) {
        checkClosed();
        final Request request = requestCreator.executeUpdate(sql);
        queRequest(request);
        return (DbSessionFuture) request.getToComplete();
    }

    @Override
    public DbSessionFuture<PreparedQuery> prepareQuery(String sql) {
        checkClosed();
        final Request request = requestCreator.executePrepareQuery(sql);
        queRequest(request);
        return (DbSessionFuture<PreparedQuery>) request.getToComplete();
    }

    @Override
    public DbSessionFuture<PreparedUpdate> prepareUpdate(String sql) {
        checkClosed();
        final Request request = requestCreator.executePrepareUpdate(sql);
        queRequest(request);
        return (DbSessionFuture<PreparedUpdate>) request.getToComplete();
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
            if(closeMode==CloseMode.CANCEL_PENDING_OPERATIONS){
                forceCloseOnPendingRequests();
            }
            Request request = requestCreator.createCloseRequest();
            forceQueRequest(request);
            closeFuture = (DefaultDbFuture<Void>) request.getToComplete();
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

    void queRequest(Request request) {
        synchronized (lock){
            int requestsPending = requestQueue.size()
                    +( (null!=blockingRequest) ? blockingRequest.waitingRequests.size() : 0);
            if(requestsPending>maxQueueSize){
                throw new DbException("To many pending requests. The current maximum is "+maxQueueSize+"."+
                    "Ensure that your not overloading the database with requests. " +
                    "Also check the "+StandardProperties.MAX_QUEUE_LENGTH+" property");
            }
            forceQueRequest(request);
        }
    }

    public void forceQueRequest(Request request) {
        synchronized (lock){
            if(blockingRequest==null){
                requestQueue.add(request);
                channel.write(request.getRequest());
                if(request.isBlocking()){
                    blockingRequest = new BlockingRequestInProgress(request);
                    request.getToComplete().addListener(new DbListener<Object>() {
                        @Override
                        public void onCompletion(DbFuture<Object> future) {
                            blockingRequest.continueWithRequests();
                        }
                    });
                }
            } else{
                if(blockingRequest.unblockBy(request)){
                    requestQueue.add(request);
                    channel.write(request.getRequest());
                } else {
                    blockingRequest.add(request);
                }
            }
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
            if(request.getRequest().wasCancelled()){
                if(logger.isDebugEnabled()){
                    logger.debug("Request has been cancelled: {}",request);
                }
                return dequeRequest();
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

    Object connectionLock(){
        return lock;
    }

    int idForAutoId() {
        return autoIdSession;
    }

    int idForCommit() {
        return commitIdSession;
    }
    int idForRollback() {
        return rollbackIdSession;
    }


    public RequestCreator requestCreator() {
        return requestCreator;
    }


    private void forceCloseOnPendingRequests() {
        for (Request request : requestQueue) {
            if(request.getRequest().tryCancel()){
                request.getToComplete().trySetException(new DbSessionClosedException("Connection is closed"));
            }
        }
        if(null!=blockingRequest){
            for (Request waitingRequest : blockingRequest.waitingRequests) {
                if(waitingRequest.getRequest().tryCancel()){
                    waitingRequest.getToComplete().trySetException(new DbSessionClosedException("Connection is closed"));
                }
            }
        }
    }

    void checkClosed(){
        if(isClosed()){
            throw new DbSessionClosedException("This connection is closed");
        }
    }
    /**
     * Expects that it is executed withing the connection lock
     */
    class BlockingRequestInProgress{
        private final ArrayList<Request> waitingRequests = new ArrayList<Request>();
        private final Request blockingRequest;

        BlockingRequestInProgress(Request blockingRequest) {
            this.blockingRequest = blockingRequest;
        }


        public void add(Request request) {
            waitingRequests.add(request);
        }

        public boolean unblockBy(Request nextRequest) {
            return blockingRequest.unblockBy(nextRequest);
        }

        public void continueWithRequests() {
            H2Connection.this.blockingRequest = null;
            for (Request waitingRequest : waitingRequests) {
                forceQueRequest(waitingRequest);
            }
        }
    }
}



