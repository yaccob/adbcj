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

public class DefaultDbFuture<T> implements DbFuture<T> {

    private final Object lock = new Object();

    private final List<DbListener<T>> otherListeners = new ArrayList<DbListener<T>>(1);

    /**
     * The result of this future.
     */
    private volatile T result;

    /**
     * The exception thrown if there was an error.
     */
    private volatile Throwable exception;

    private volatile FutureState state = FutureState.NOT_COMPLETED;

    private final CancellationAction optionalCancellation;



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
        synchronized (lock) {
            if (isDone()) {
                notifyListener(listener);
            } else{
                otherListeners.add(listener);
            }

        }
        return this;
    }

    public boolean removeListener(DbListener<T> listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener can NOT be null");
        }

        synchronized (lock) {
            return otherListeners.remove(listener);
        }
    }

    public final boolean cancel(boolean mayInterruptIfRunning) {
        if (isDone() || optionalCancellation==null) {
            return false;
        }
        synchronized (lock) {
            if (isDone()) {
                return false;
            }
            boolean cancelled = optionalCancellation.cancel();
            if (cancelled) {
                state = FutureState.CANCELLED;
                notifyChanges();
            }
            return cancelled;
        }
    }



    public final T get() throws InterruptedException, DbException {
        if (isDone()) {
            return getResult();
        }
        synchronized (lock) {
            if (isDone()) {
                return getResult();
            }
            while (!isDone()) {
                lock.wait();
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
        synchronized (lock) {
            final long startTime = System.nanoTime();
            while (!isDone() &&(startTime+timeoutNanos) > System.nanoTime() ){
                lock.wait(timeoutMillis);
            }
            if (!isDone()) {
                throw new TimeoutException();
            }
        }
        return getResult();
    }


    public final T getResult() throws DbException {
        if (state == FutureState.SUCCESS) {
            return result;
        }
        else if (state == FutureState.NOT_COMPLETED) {
            throw new IllegalStateException("Should not be calling this method when future is not done");
        }
        else if (state == FutureState.FAILURE) {
            throw DbException.wrap(exception);
        }
        else if (state == FutureState.CANCELLED) {
            throw new CancellationException();
        }
        return result;
    }

    @Override
    public FutureState getState() {
        return state;
    }

    @Override
    public Throwable getException() {
        if(state==FutureState.FAILURE){
            return exception;
        } else if(state==FutureState.NOT_COMPLETED){
            throw new IllegalStateException("Should not be calling this method when future is not done");
        } else{
            return null;
        }
    }

    public final void setResult(T result) {
        if (!trySetResult(result)) {
            throw new IllegalStateException("Should not set result if future is completed");
        }
    }

    public boolean trySetResult(T result) {
        synchronized (lock) {
            if (isDone()) {
                return false;
            }

            this.result = result;
            state = FutureState.SUCCESS;
            notifyChanges();
            return true;
        }

    }

    private void notifyListener(DbListener<T> listener) {
        listener.onCompletion(this);
    }

    private void notifyChanges() {
        synchronized (lock) {
            lock.notifyAll();
            if (otherListeners != null) {
                for (DbListener<T> l : otherListeners) {
                    notifyListener(l);
                }
                otherListeners.clear();
            }
        }
    }

    public boolean isCancelled() {
        return state==FutureState.CANCELLED;
    }

    public boolean isDone() {
        return state!=FutureState.NOT_COMPLETED;
    }

    public void setException(Throwable exception) {
        if(!trySetException(exception)){
            throw new IllegalStateException("Can't set exception on completed future");
        }
    }

    /**
     * Try to complete this future with an exception. If wasn't completed yet, the future
     * will fail with the given exception and the method return true. Otherwise this method
     * doesn't change the state of the future and return false.
     * @param exception
     * @return true when state of future could be changed to a failure. False otherwise
     */
    public boolean trySetException(Throwable exception) {
        synchronized (lock) {
            if (isDone()) {
                return false;
            }
            this.exception = exception;
            state = FutureState.FAILURE;
            notifyChanges();
            return true;
        }
    }


    boolean trySetCancelled() {
        if(state!=FutureState.NOT_COMPLETED){
            return false;
        }
        synchronized (this.lock){
            state = FutureState.CANCELLED;
            notifyChanges();
            return true;
        }
    }


}
