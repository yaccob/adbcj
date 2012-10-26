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
package org.adbcj.support;

import org.adbcj.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class AbstractDbSession implements DbSession {

    protected final Object lock = this;

    protected final Queue<Request<?>> requestQueue = new ConcurrentLinkedQueue<Request<?>>();

    private Request<?> activeRequest; // Access must by synchronized on lock

    private Transaction transaction; // Access must by synchronized on lock

    private boolean pipelining = false; // Access must be synchronized on lock

    protected AbstractDbSession() {
    }

    protected <E> Request<E> enqueueRequest(final Request<E> request) {
        // Check to see if the request can be pipelined
        synchronized (lock) {
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
            requestQueue.add(request);
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
            if (activeRequest != null && !activeRequest.isDone()) {
                activeRequest.error(DbException.wrap(exception));
            }
            Request<?> request = requestQueue.poll();
            while(null!=request){
                if (!request.isDone()) {
                    request.error(DbException.wrap(exception));
                }

                request = requestQueue.poll();
            }
        }
    }

    /**
     * Throws {@link DbSessionClosedException} if session is closed
     *
     * @throws if {@link DbSession} is closed.
     */
    protected abstract void checkClosed() throws DbSessionClosedException;

    public DbSessionFuture<ResultSet> executeQuery(String sql) {
        ResultHandler<DefaultResultSet> eventHandler = new DefaultResultEventsHandler();
        DefaultResultSet resultSet = new DefaultResultSet();
        return executeQuery0(sql, eventHandler, resultSet);
    }

    @SuppressWarnings("unchecked")
    private <T extends ResultSet> DbSessionFuture<ResultSet> executeQuery0(String sql, ResultHandler<T> eventHandler, T accumulator) {
        return (DbSessionFuture<ResultSet>) executeQuery(sql, eventHandler, accumulator);
    }

    //*****************************************************************************************************************
    //
    //  Transaction methods
    //
    //*****************************************************************************************************************

    public boolean isInTransaction() {
        checkClosed();
        synchronized (lock) {
            return transaction != null;
        }
    }

    public void beginTransaction() {
        checkClosed();
        synchronized (lock) {
            if (isInTransaction()) {
                throw new DbException("Cannot begin new transaction.  Current transaction needs to be committed or rolled back");
            }
            transaction = new Transaction();
        }
    }

    public DbSessionFuture<Void> commit() {
        checkClosed();
        if (!isInTransaction()) {
            throw new DbException("Not currently in a transaction, cannot commit");
        }
        DbSessionFuture<Void> future;
        synchronized (lock) {
            if (transaction.isBeginScheduled()) {
                future = enqueueCommit(transaction).getFuture();
                markTransactionAsCompleteWhenDone(future);
                return future;
            } else {
                transaction = null;
                // If transaction was not started, don't worry about committing transaction
                future = DefaultDbSessionFuture.createCompletedFuture(this, null);
            }
        }
        return future;
    }


    public DbSessionFuture<Void> rollback() {
        checkClosed();
        if (!isInTransaction()) {
            throw new DbException( "Not currently in a transaction, cannot rollback");
        }
        DbSessionFuture<Void> future;
        synchronized (lock) {
            if (transaction.isBeginScheduled()) {
                transaction.cancelPendingRequests();
                future = enqueueRollback(transaction).getFuture();
                markTransactionAsCompleteWhenDone(future);
            } else {
                this.transaction = null;
                future = DefaultDbSessionFuture.createCompletedFuture(this, null);
            }
        }
        return future;
    }

    public <E> DbSessionFuture<E> enqueueTransactionalRequest(Request<E> request) {
        // Check to see if we're in a transaction
        synchronized (lock) {
            if (transaction != null) {
                if (transaction.isCanceled()) {
                    return DefaultDbSessionFuture.createCompletedErrorFuture(
                            this, new DbException("Could not execute request; transaction is in failed state"));
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
    private void markTransactionAsCompleteWhenDone(DbSessionFuture<Void> future) {
        future.addListener(new DbListener<Void>() {
            @Override
            public void onCompletion(DbFuture<Void> voidDbFuture) throws Exception {
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
        public void tryComplete(Void result) {
            // Avoid casting exception in case
            // the underlying implementation
            // actually returns a result
            super.tryComplete(null);
        }

        @Override
        public boolean cancelRequest(boolean mayInterruptIfRunning) {
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
        public void tryComplete(Void result) {
            // Avoid casting exception in case
            // the underlying implementation
            // actually returns a result
            super.tryComplete(null);
        }

        @Override
        // Return false because a rollback cannot be cancelled
        public boolean cancelRequest(boolean mayInterruptIfRunning) {
            return false;
        }
    }

    public static abstract class Request<T>  {
        private volatile Transaction transaction;
        private final DefaultDbSessionFuture<T> futureToComplete;
        private final AbstractDbSession session;

        private boolean cancelled; // Access must be synchronized on this
        private boolean executed; // Access must be synchronized on this


        public Request(AbstractDbSession session) {
            this.futureToComplete = new DefaultDbSessionFuture<T>(session){
                @Override
                protected boolean doCancel(boolean mayInterruptIfRunning) {
                    return Request.this.doCancel(mayInterruptIfRunning);
                }
            };
            this.session = session;
        }

        /**
         * Checks to see if the request has been cancelled, if not invokes the execute method.  If pipelining, this
         * method ensures the request does not get executed twice.
         *
         * @throws Exception
         */
        public final synchronized void invokeExecute() throws Exception {
            if (cancelled || executed) {
                synchronized (session.lock) {
                    if (futureToComplete.isDone() && session.getActiveRequest() == this) {
                        session.makeNextRequestActive();
                    }
                }
            } else {
                executed = true;
                execute();
            }
        }

        public final synchronized boolean doCancel(boolean mayInterruptIfRunning) {
            if (executed) {
                return false;
            }
            cancelled = cancelRequest(mayInterruptIfRunning);

            // The the request was cancelled and it can be removed
            if (cancelled && canRemove()) {
                    // Remove the quest and if the removal was successful and this request is active, go to the next request
                    synchronized (session.lock) {
                    if (session.requestQueue.remove(this)) {
                        if (this == session.getActiveRequest()) {
                            session.makeNextRequestActive();
                        }
                    }
                }
                return cancelled;
            } else{
                throw new Error("Not expected branch");
            }
        }

        protected abstract void execute() throws Exception;

        protected boolean cancelRequest(boolean mayInterruptIfRunning) {
            return true;
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
            futureToComplete.setResult(result);
            makeNextRequestActive();
        }

        public void tryComplete(T result) {
            futureToComplete.trySetResult(result);
            makeNextRequestActive();
        }

        public boolean isDone() {
            return futureToComplete.isDone();
        }

        public void error(DbException exception) {
            futureToComplete.setException(exception);
            if (transaction != null) {
                transaction.cancelPendingRequests();
            }
            makeNextRequestActive();
        }

        private void makeNextRequestActive() {
            synchronized (session.lock) {
                if (session.getActiveRequest() == this) {
                    session.makeNextRequestActive();
                }
            }
        }

        public DbSessionFuture<T> getFuture(){
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
        private final List<Request<?>> requests = new LinkedList<Request<?>>();

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
