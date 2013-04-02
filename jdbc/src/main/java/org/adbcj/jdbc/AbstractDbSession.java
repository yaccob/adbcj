/*
 *   Copyright (c) 2007 Mike Heath.  All rights reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package org.adbcj.jdbc;

import org.adbcj.*;
import org.adbcj.support.*;
import org.adbcj.support.stacktracing.StackTracingOptions;
import org.slf4j.Logger;

import java.util.*;

/**
 * @deprecated To complex. Need to be more like H2 driver. Better not abstract implementation mabye?
 */
public abstract class AbstractDbSession implements DbSession {

    private final Object lock = this;

    private final Queue<Request<?>> requestQueue; // Access must by synchronized on lock

    private Request<?> activeRequest; // Access must by synchronized on lock

    private Transaction transaction; // Access must by synchronized on lock
    private boolean isInTransaction; // Access must by synchronized on lock

    private boolean pipelining = false; // Access must be synchronized on lock

    private final StackTracingOptions stackTracingOptions;
    private final int maxQueueSize;


    protected abstract Logger logger();

    protected AbstractDbSession(StackTracingOptions stackTracingOptions,int maxQueueSize) {
        this.stackTracingOptions = stackTracingOptions;
        this.maxQueueSize = maxQueueSize;
        synchronized (lock){
            requestQueue = new ArrayDeque<Request<?>>(maxQueueSize+1);
        }
    }

    protected <E> Request<E> enqueueRequest(final Request<E> request) {
        // Check to see if the request can be pipelined
        synchronized (lock) {
            if(requestQueue.size()>=maxQueueSize){
                throw new DbException("To many pending requests. The current maximum is "+maxQueueSize+"."+
                    "Ensure that your not overloading the database with requests. " +
                        "Also check the "+StandardProperties.MAX_QUEUE_LENGTH+" property");
            }
            if (request.isPipelinable()) {
                // Check to see if we're in a piplinging state
                if (pipelining) {
                    invokeExecuteWithCatch(request);
                    // If the request errors out on execution, return
                    if (request.isDone()) {
                        return request;
                    }

                }
            } else {
                pipelining = false;
            }
            requestQueue.offer(request);
            if (activeRequest == null) {
                makeNextRequestActive();
            }
        }
        return request;
    }

    @SuppressWarnings("unchecked")
    protected final <E> Request<E> makeNextRequestActive() {
        Request<E> request;
        boolean executePipelining = false;
        synchronized (lock) {
            if (activeRequest != null && !activeRequest.isDone()) {
                throw new ActiveRequestIncomplete("Active request is not done: " + activeRequest);
            }
            request = (Request<E>) requestQueue.poll();

            if(request!=null){
                if (request.isPipelinable()) {
                    executePipelining = !pipelining;
                } else {
                    pipelining = false;
                }
            }

            activeRequest = request;
        }
        if (request != null) {
            invokeExecuteWithCatch(request);
        }

        // Check if we need to execute pending pipelinable requests
        if (executePipelining) {
            synchronized (lock) {
                // Iterate over queue
                Iterator<Request<?>> iterator = requestQueue.iterator();
                while (iterator.hasNext()) {
                    Request<?> next = iterator.next();
                    if (next.isPipelinable()) {
                        invokeExecuteWithCatch(next);

                        // If there are no more requests to iterate over, put DbSession in pipelining enabled state
                        if (!iterator.hasNext()) {
                            pipelining = true;
                        }
                    } else {
                        // Stop looping once we find a non pipelinable request.
                        break;
                    }
                }
            }

        }

        return request;
    }

    protected <E> void invokeExecuteWithCatch(Request<E> request) {
        try {
            request.invokeExecute();
        } catch (Throwable e) {
            request.error(DbException.wrap(e));
            request.complete(null);
        }
    }

    @SuppressWarnings("unchecked")
    protected <E> Request<E> getActiveRequest() {
        synchronized (lock) {
            return (Request<E>) activeRequest;
        }
    }

    /**
     * This will error out any pending requests.
     */
    public void errorPendingRequests(Throwable exception) {
        synchronized (lock) {
            Request<?> request = requestQueue.poll();
            while(null!=request){
                if (!request.isDone()) {
                    request.error(DbException.wrap(exception));
                }

                request = requestQueue.poll();
            }
            if (activeRequest != null && !activeRequest.isDone()) {
                activeRequest.error(DbException.wrap(exception));
            }
        }
    }

    public StackTracingOptions stackTracingOptions(){
        return stackTracingOptions;
    }

    /**
     * Throws {@link DbSessionClosedException} if session is closed
     *
     * @throws if {@link DbSession} is closed.
     */
    protected abstract void checkClosed() throws DbSessionClosedException;

    public DbFuture<ResultSet> executeQuery(String sql) {
        ResultHandler<DefaultResultSet> eventHandler = new DefaultResultEventsHandler();
        DefaultResultSet resultSet = new DefaultResultSet();
        return (DbFuture) executeQuery(sql, eventHandler, resultSet);
    }


    //*****************************************************************************************************************
    //
    //  Transaction methods
    //
    //*****************************************************************************************************************

    public boolean isInTransaction() {
        checkClosed();
        synchronized (lock) {
            return isInTransaction;
        }
    }

    public void beginTransaction() {
        checkClosed();
        synchronized (lock) {
            if (isInTransaction()) {
                throw new DbException("Cannot begin new transaction.  Current transaction needs to be committed or rolled back");
            }
            transaction = new Transaction();
            isInTransaction = true;
        }
    }

    public DbFuture<Void> commit() {
        checkClosed();
        if (!isInTransaction()) {
            throw new DbException("Not currently in a transaction, cannot commit");
        }
        DbFuture<Void> future;
        synchronized (lock) {
            isInTransaction = false;
            if (transaction.isBeginScheduled()) {
                future = enqueueCommit(transaction).getFuture();
                markTransactionAsCompleteWhenDone(future);
                return future;
            } else {
                transaction = null;
                // If transaction was not started, don't worry about committing transaction
                future = DefaultDbFuture.completed(null);
            }
        }
        return future;
    }


    public DbFuture<Void> rollback() {
        checkClosed();
        if (!isInTransaction()) {
            throw new DbException( "Not currently in a transaction, cannot rollback");
        }
        DbFuture<Void> future;
        synchronized (lock) {
            isInTransaction = false;
            if (transaction.isBeginScheduled()) {
                transaction.cancelPendingRequests();
                future = enqueueRollback(transaction).getFuture();
                markTransactionAsCompleteWhenDone(future);
            } else {
                this.transaction = null;
                future = DefaultDbFuture.completed(null);
            }
        }
        return future;
    }

    public <E> DbFuture<E> enqueueTransactionalRequest(Request<E> request) {
        // Check to see if we're in a transaction
        synchronized (lock) {
            if (transaction != null) {
                if (transaction.isCanceled()) {
                    return DefaultDbFuture.createCompletedErrorFuture(
                            this.stackTracingOptions(), new DbException("Could not execute request; transaction is in failed state"));
                }
                // Schedule starting transaction with database if possible
                if (!transaction.isBeginScheduled()) {
                    // Set isolation level if necessary
                    enqueueStartTransaction(transaction);
                    transaction.setBeginScheduled(true);
                }
                transaction.addRequest(request);
            }
        }
        enqueueRequest(request);
        return request.getFuture();
    }

    private Request<Void> enqueueStartTransaction(final Transaction transaction) {
        Request<Void> request = createBeginRequest(transaction);
        enqueueTransactionalRequest(transaction, request);
        return request;
    }

    private Request<Void> enqueueCommit(final Transaction transaction) {
        Request<Void> request = createCommitRequest(transaction);
        enqueueTransactionalRequest(transaction, request);
        return request;

    }

    private Request<Void> enqueueRollback(Transaction transaction) {
        Request<Void> request = createRollbackRequest();
        enqueueTransactionalRequest(transaction, request);
        return request;
    }

    private void enqueueTransactionalRequest(final Transaction transaction, Request<Void> request) {
        enqueueRequest(request);
        transaction.addRequest(request);
    }

    // Rollback cannot be cancelled or removed
    protected Request<Void> createRollbackRequest() {
        return new RollbackRequest();
    }

    protected abstract void sendBegin() throws Exception;

    protected abstract void sendCommit() throws Exception;

    protected abstract void sendRollback() throws Exception;

    protected Request<Void> createBeginRequest(final Transaction transaction) {
        return new BeginRequest(AbstractDbSession.this, transaction);
    }

    protected Request<Void> createCommitRequest(final Transaction transaction) {
        return new CommitRequest(this, transaction);
    }
    private void markTransactionAsCompleteWhenDone(DbFuture<Void> future) {
        future.addListener(new DbListener<Void>() {
            @Override
            public void onCompletion(DbFuture<Void> voidDbFuture) {
                synchronized (lock){
                    transaction = null;
                }
            }
        });
    }

    /**
     * Default request for starting a transaction.
     */
    protected class BeginRequest extends Request<Void> {
        private final Transaction transaction;

        private BeginRequest(AbstractDbSession session, Transaction transaction) {
            super(session);
            if (transaction == null) {
                throw new IllegalArgumentException("transaction can NOT be null");
            }
            this.transaction = transaction;
        }

        @Override
        public void execute() throws Exception {
            transaction.setStarted(true);
            sendBegin();
        }
    }

    /**
     * Default request for committing a transaction.
     */
    protected class CommitRequest extends Request<Void> {
        private final Transaction transaction;

        private CommitRequest(AbstractDbSession session, Transaction transaction) {
            super(session);
            if (transaction == null) {
                throw new IllegalArgumentException("transaction can NOT be null");
            }
            this.transaction = transaction;
        }

        public void execute() throws Exception {
            if (getFuture().isCancelled()) {
                // If the transaction has started, send a rollback
                if (transaction.isStarted()) {
                    sendRollback();
                }
            } else {
                sendCommit();
            }
        }

        @Override
        public boolean cancelRequest() {
            transaction.cancelPendingRequests();
            return true;
        }

        @Override
        public boolean canRemove() {
            return false;
        }

        @Override
        public boolean isPipelinable() {
            return false;
        }
    }

    /**
     * Default request for rolling back a transaction.
     */
    protected class RollbackRequest extends Request<Void> {
        public RollbackRequest() {
            super(AbstractDbSession.this);
        }

        @Override
        public void execute() throws Exception {
            sendRollback();
        }

        @Override
        // Return false because a rollback cannot be cancelled
        public boolean cancelRequest() {
            return false;
        }
    }

    public static abstract class Request<T>  {
        private volatile Transaction transaction;
        private final DefaultDbFuture<T> futureToComplete;
        private final AbstractDbSession session;

        private boolean cancelled; // Access must be synchronized on this
        private boolean executed; // Access must be synchronized on this
        private final Logger logger;


        public Request(AbstractDbSession session) {
            this.futureToComplete = new DefaultDbFuture<T>(session.stackTracingOptions(),new CancellationAction() {
                @Override
                public boolean cancel() {
                    return Request.this.doCancel();
                }
            });
            this.session = session;
            this.logger = session.logger();
        }

        /**
         * Checks to see if the request has been cancelled, if not invokes the execute method.  If pipelining, this
         * method ensures the request does not get executed twice.
         *
         * @throws Exception
         */
        public final void invokeExecute() throws Exception {
            synchronized (session.lock){
                if (executed) {
                    if (futureToComplete.isDone() && session.getActiveRequest() == this) {
                        session.makeNextRequestActive();
                    }
                } else {
                    if(logger.isDebugEnabled()){
                        logger.debug("Request is sent to database: "+this);
                    }
                    executed = true;
                    execute();
                }
            }
        }

        public final boolean doCancel() {
            synchronized (session.lock){
                if (executed) {
                    return false;
                }
                cancelled = cancelRequest();

                // The the request was cancelled and it can be removed
                if (cancelled && canRemove()) {
                    if (session.requestQueue.remove(this)) {
                        if (this == session.getActiveRequest()) {
                            session.makeNextRequestActive();
                        }
                    }
                    return cancelled;
                } else{
                    throw new RuntimeException("Not expected branch");
                }
            }
        }

        protected abstract void execute() throws Exception;

        protected boolean cancelRequest() {
            return !executed;
        }

        public boolean canRemove() {
            return true;
        }

        public boolean isPipelinable() {
            return true;
        }

        public void setTransaction(Transaction transaction) {
            this.transaction = transaction;
        }


        public void complete(T result) {
            futureToComplete.trySetResult(result);
            if(logger.isDebugEnabled()){
                logger.debug("Request has been completed: "+this + " the future is in state: "+this.getFuture());
            }
            makeNextRequestActive();
        }

        public boolean isDone() {
            return futureToComplete.isDone();
        }

        public void error(DbException exception) {
            futureToComplete.trySetException(exception);
            if (transaction != null) {
                transaction.cancelPendingRequests();
            }
        }

        private void makeNextRequestActive() {
            synchronized (session.lock) {
                if (session.getActiveRequest() == this) {
                    session.makeNextRequestActive();
                }
            }
        }

        public DbFuture<T> getFuture(){
            return this.futureToComplete;
        }

        public boolean cancel(boolean mayInterrupt){
            return getFuture().cancel(mayInterrupt);
        }
    }


    public static class Transaction {

        private volatile boolean started = false;
        private volatile boolean beginScheduled = false;
        private volatile boolean canceled = false;
        private final List<Request<?>> requests = new ArrayList<Request<?>>();

        /**
         * Indicates if the transaction has been started on the server (i.e. if 'begin' has been sent to server)
         *
         * @return true if 'begin' has been sent to the server, false otherwise
         */
        public boolean isStarted() {
            return started;
        }

        public void setStarted(boolean started) {
            this.started = started;
        }

        /**
         * Indicates if 'begin' has been scheduled to be sent to remote database server but not necessarily sent.
         *
         * @return true if 'begin' has been queued to be sent to the remote database server, false otherwise.
         */
        public boolean isBeginScheduled() {
            return beginScheduled;
        }

        public void setBeginScheduled(boolean beginScheduled) {
            this.beginScheduled = beginScheduled;
        }

        public void addRequest(Request<?> request) {
            request.setTransaction(this);
            synchronized (requests) {
                requests.add(request);
            }
        }

        public boolean isCanceled() {
            return canceled;
        }

        public void cancelPendingRequests() {
            canceled = true;
            synchronized (requests) {
                for (Request<?> request : requests) {
                    request.cancel(false);
                }
            }
        }

    }

}
