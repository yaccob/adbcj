package org.adbcj.h2;

import org.adbcj.*;
import org.adbcj.support.*;
import io.netty.channel.Channel;
import org.adbcj.support.stacktracing.StackTracingOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicInteger;


public class H2Connection implements Connection {
    private static final Logger logger = LoggerFactory.getLogger(H2Connection.class);
    private final String sessionId = StringUtils.convertBytesToHex(MathUtils.secureRandomBytes(32));
    private final ArrayDeque<Request> requestQueue;
    private final int maxQueueSize;
    private final H2ConnectionManager manager;
    private final Channel channel;
    private final Object lock = new Object();
    final StackTracingOptions strackTraces;
    private final AtomicInteger requestId = new AtomicInteger(0);
    private final int autoIdSession = nextId();
    private final int commitIdSession = nextId();
    private final int rollbackIdSession = nextId();
    /**
     * {@see BlockingRequestInProgress} for explanation
     */
    BlockingRequestInProgress blockingRequest;

    private volatile boolean isInTransaction = false;
    private final CloseOnce closer = new CloseOnce();

    private final RequestCreator requestCreator = new RequestCreator(this);

    public H2Connection(int maxQueueSize, H2ConnectionManager manager, Channel channel, StackTracingOptions strackTraces) {
        this.maxQueueSize = maxQueueSize;
        this.manager = manager;
        this.channel = channel;
        requestQueue = new ArrayDeque<>(maxQueueSize + 1);
        this.strackTraces = strackTraces;
    }


    @Override
    public H2ConnectionManager getConnectionManager() {
        return manager;
    }

    public void beginTransaction(DbCallback<Void> callback) {
        checkClosed();
        StackTraceElement[] entry = strackTraces.captureStacktraceAtEntryPoint();
        synchronized (lock) {
            if (isInTransaction()) {
                throw new DbException("Cannot begin new transaction.  Current transaction needs to be committed or rolled back");
            }
            isInTransaction = true;
            final Request request = requestCreator.beginTransaction(callback, entry);
            queRequest(request);
        }
    }

    public void commit(DbCallback<Void> callback) {
        checkClosed();
        StackTraceElement[] entry = strackTraces.captureStacktraceAtEntryPoint();
        synchronized (lock) {
            if (!isInTransaction()) {
                throw new DbException("Not currently in a transaction, cannot commit");
            }
            final Request request = requestCreator.commitTransaction(callback, entry);
            queRequest(request);
            isInTransaction = false;
        }
    }

    public void rollback(DbCallback<Void> callback) {
        checkClosed();
        StackTraceElement[] entry = strackTraces.captureStacktraceAtEntryPoint();
        synchronized (lock) {
            if (!isInTransaction()) {
                throw new DbException("Not currently in a transaction, cannot rollback");
            }
            final Request request = requestCreator.rollbackTransaction(callback, entry);
            queRequest(request);
            isInTransaction = false;
        }
    }

    @Override
    public boolean isInTransaction() {
        return isInTransaction;
    }

    public <T> void executeQuery(String sql, ResultHandler<T> eventHandler, T accumulator, DbCallback<T> callback) {
        checkClosed();
        StackTraceElement[] entry = strackTraces.captureStacktraceAtEntryPoint();

        Request request = requestCreator.createQuery(sql, eventHandler, accumulator, callback, entry);
        queRequest(request);
    }

    public void executeUpdate(String sql, DbCallback<Result> callback) {
        checkClosed();
        StackTraceElement[] entry = strackTraces.captureStacktraceAtEntryPoint();

        Request request =requestCreator.createUpdate(sql, callback, entry);
        queRequest(request);
    }

    public void prepareQuery(String sql, DbCallback<PreparedQuery> callback) {
        checkClosed();
        StackTraceElement[] entry = strackTraces.captureStacktraceAtEntryPoint();
        final Request request = requestCreator.executePrepareQuery(sql, callback, entry);
        queRequest(request);
    }

    public void prepareUpdate(String sql, DbCallback<PreparedUpdate> callback) {
        checkClosed();
        StackTraceElement[] entry = strackTraces.captureStacktraceAtEntryPoint();
        final Request request = requestCreator.executePrepareUpdate(sql, callback, entry);
        queRequest(request);
    }

    public void close(CloseMode closeMode, DbCallback<Void> callback) throws DbException {
        synchronized (lock) {
            closer.requestClose(callback, () -> {
                if (closeMode == CloseMode.CANCEL_PENDING_OPERATIONS) {
                    forceCloseOnPendingRequests();
                }
                StackTraceElement[] entry = strackTraces.captureStacktraceAtEntryPoint();
                Request request = requestCreator.createCloseRequest(
                        (result, error) -> {
                            tryCompleteClose(error);
                        },
                        entry);
                forceQueRequest(request);
            });
        }
    }

    @Override
    public boolean isClosed() throws DbException {
        return closer.isClose();
    }

    void queRequest(Request request) {
        synchronized (lock) {
            throwIfNoSpaceInQueue();
            forceQueRequest(request);
        }
    }

    void throwIfNoSpaceInQueue() {
        int requestsPending = requestQueue.size() + (blockingRequest==null?0:blockingRequest.size());
        if (requestsPending > maxQueueSize) {
            throw new DbException("To many pending requests. The current maximum is " + maxQueueSize + "." +
                    "Ensure that your not overloading the database with requests. " +
                    "Also check the " + StandardProperties.MAX_QUEUE_LENGTH + " property");
        }
    }

    void cancelBlockedRequest(Request request) {
        synchronized (lock) {
            assert blockingRequest!=null;
            assert blockingRequest.unblockBy(request);
            BlockingRequestInProgress req = blockingRequest;
            blockingRequest = null;
            req.continueWithRequests();
        }

    }

    public void forceQueRequest(Request request) {
        synchronized (lock) {
            if (blockingRequest == null) {
                requestQueue.add(request);
                channel.writeAndFlush(request.getRequest());
                if (request instanceof BlockingRequestInProgress) {
                    blockingRequest = (BlockingRequestInProgress) request;
                }
            } else {
                if (blockingRequest.unblockBy(request)) {
                    requestQueue.add(request);
                    channel.writeAndFlush(request.getRequest());
                    BlockingRequestInProgress req = blockingRequest;
                    blockingRequest = null;
                    req.continueWithRequests();
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
        synchronized (lock) {
            final Request request = requestQueue.poll();
            if (logger.isDebugEnabled()) {
                logger.debug("Dequeued request: {}", request);
            }
            return request;
        }
    }

    public int nextId() {
        return requestId.incrementAndGet();
    }

    public void tryCompleteClose(DbException error) {
        synchronized (lock) {
            H2Connection.this.manager.closedConnection(H2Connection.this);
            closer.didClose(error);
        }
    }

    Object connectionLock() {
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
        synchronized (lock){
            DbConnectionClosedException closedEx = new DbConnectionClosedException("Connection closed");
            if(blockingRequest!=null){
                blockingRequest.completeFailure(closedEx);
            }
            for (Request request : requestQueue) {
                if(blockingRequest!=request){
                    request.completeFailure(closedEx);
                }
            }
        }
    }

    void checkClosed() {
        if (isClosed()) {
            throw new DbConnectionClosedException("This connection is closed");
        }
    }

}



