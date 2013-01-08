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

import org.adbcj.DbException;
import org.adbcj.DbFuture;
import org.adbcj.DbListener;
import org.adbcj.FutureState;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultDbFuture<T> implements DbFuture<T> {

    private final List<DbListener<T>> otherListeners = new ArrayList<DbListener<T>>(1);

    private final CancellationAction optionalCancellation;

    private final AtomicReference<MyState> state = new AtomicReference<MyState>(NotCompleted.NOT_COMPLETED);


    public DefaultDbFuture(CancellationAction cancelAction) {
        this.optionalCancellation = cancelAction;
    }

    public DefaultDbFuture() {
        this.optionalCancellation = null;
    }


    public static <T> DbFuture<T> completed(T result) {
        DefaultDbFuture f = new DefaultDbFuture();
        f.setResult(result);
        return f;
    }

    public DbFuture<T> addListener(DbListener<T> listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener can NOT be null");
        }
        synchronized (otherListeners) {
            if (isDone()) {
                notifyListener(listener);
            } else {
                otherListeners.add(listener);

            }
        }
        return this;
    }

    public boolean removeListener(DbListener<T> listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener can NOT be null");
        }

        synchronized (otherListeners) {
            return otherListeners.remove(listener);
        }
    }

    public final boolean cancel(boolean mayInterruptIfRunning) {
        if (optionalCancellation == null) {
            return false;
        }
        synchronized (optionalCancellation){
            if(isDone()){
                return false;
            }
            boolean cancelled = optionalCancellation.cancel();
            if(cancelled){
                return tryStateTransition(Cancelled.CANCELLED)
                        // Internal transformation may called #trySetCancelled while optionalCancellation.cancel()
                        // Therefore we also accept if where in the cancel state now
                        || state.get().getState()==FutureState.CANCELLED;
            } else {
                return false;
            }
        }
    }


    public final T get() throws InterruptedException, DbException {
        if (isDone()) {
            return getResult();
        }
        synchronized (this) {
            while (!isDone()) {
                this.wait();
            }
        }
        return getResult();
    }

    public final T get(long timeout, TimeUnit unit) throws InterruptedException, DbException, TimeoutException {
        long timeoutMillis = unit.toMillis(timeout);
        long timeoutNanos = unit.toNanos(timeout);

        if (isDone()) {
            return getResult();
        }
        synchronized (this) {
            final long startTime = System.nanoTime();
            while (!isDone() && (startTime + timeoutNanos) > System.nanoTime()) {
                this.wait(timeoutMillis);
            }
            if (!isDone()) {
                throw new TimeoutException();
            }
        }
        return getResult();
    }


    public final T getResult() throws DbException {
        final MyState myState = state.get();
        final FutureState futureState = myState.getState();
        switch (futureState) {
            case SUCCESS:
                return ((Completed<T>) myState).getData();
            case NOT_COMPLETED:
                throw new IllegalStateException("Should not be calling this method when future is not done");
            case FAILURE:
                throw DbException.wrap(((Failed) myState).getError());
            case CANCELLED:
                throw new CancellationException();
            default:
                throw new Error("Implementation error. Should be unreachable");
        }
    }

    @Override
    public FutureState getState() {
        return state.get().getState();
    }

    @Override
    public Throwable getException() {
        MyState stateOfFuture = state.get();
        if (stateOfFuture.getState() == FutureState.FAILURE) {
            return ((Failed)stateOfFuture).getError();
        } else if (stateOfFuture.getState() == FutureState.NOT_COMPLETED) {
            throw new IllegalStateException("Should not be calling this method when future is not done");
        } else {
            return null;
        }
    }

    public final void setResult(T result) {
        if (!trySetResult(result)) {
            throw new IllegalStateException("Should not set result if future is completed");
        }
    }

    public boolean trySetResult(T result) {
        return tryStateTransition(new Completed<T>(result));
    }

    private void notifyListener(DbListener<T> listener) {
        listener.onCompletion(this);
    }

    private void notifyChanges() {
        List<DbListener<T>> listenersToCall;
        synchronized (otherListeners) {
            listenersToCall = new ArrayList<DbListener<T>>(otherListeners);
            otherListeners.clear();
        }
        for (DbListener<T> l : listenersToCall) {
            notifyListener(l);
        }
        synchronized (this){
            this.notifyAll();
        }
    }

    public boolean isCancelled() {
        return state.get().getState() == FutureState.CANCELLED;
    }

    public boolean isDone() {
        return state.get().getState() != FutureState.NOT_COMPLETED;
    }

    public void setException(Throwable exception) {
        if (!trySetException(exception)) {
            throw new IllegalStateException("Can't set exception on completed future");
        }
    }

    @Override
    public String toString() {
        return "DbFuture{" + state + '}';
    }

    /**
     * Try to complete this future with an exception. If wasn't completed yet, the future
     * will fail with the given exception and the method return true. Otherwise this method
     * doesn't change the state of the future and return false.
     *
     * @param exception
     * @return true when state of future could be changed to a failure. False otherwise
     */
    public boolean trySetException(Throwable exception) {
        return tryStateTransition(new Failed(exception));
    }


    boolean trySetCancelled() {
        return tryStateTransition(Cancelled.CANCELLED);
    }


    private boolean tryStateTransition(MyState newState){
        MyState currentState = state.get();
        if(currentState.getState()!=FutureState.NOT_COMPLETED){
            return false;
        }
        /**
         * Remember, the future can only have on state transition
         * There if it fails, we're too late and don't change the state at all
         */
        boolean changedState = state.compareAndSet(currentState, newState);
        if(changedState){
            notifyChanges();
        }
        return changedState;
    }


    static abstract class MyState {

        private final FutureState state;

        protected MyState(FutureState state) {
            this.state = state;
        }

        public FutureState getState() {
            return state;
        }

        @Override
        public String toString(){
            return state.toString();
        }
    }

    static class NotCompleted extends MyState {
        private static NotCompleted NOT_COMPLETED = new NotCompleted();

        private NotCompleted() {
            super(FutureState.NOT_COMPLETED);
        }
    }

    static class Completed<T> extends MyState {
        private final T data;

        public Completed(T result) {
            super(FutureState.SUCCESS);
            this.data = result;
        }

        public T getData() {
            return data;
        }
        @Override
        public String toString(){
            return super.toString() + " with "+String.valueOf(data);
        }
    }

    static class Failed extends MyState {
        private final Throwable error;

        public Failed(Throwable result) {
            super(FutureState.FAILURE);
            this.error = result;
        }

        public Throwable getError() {
            return error;
        }
        @Override
        public String toString(){
            return super.toString() + " with "+String.valueOf(error);
        }
    }

    static class Cancelled extends MyState {
        private static Cancelled CANCELLED = new Cancelled();

        private Cancelled() {
            super(FutureState.CANCELLED);
        }
    }
}
