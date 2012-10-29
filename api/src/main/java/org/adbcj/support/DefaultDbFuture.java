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

    /**
     * Indicates if the future was cancelled.
     */
    private volatile boolean cancelled;

    /**
     * Indicates if the future has completed or not.
     */
    private volatile boolean done;

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
            if (done) {
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
        if (done || optionalCancellation==null) {
            return false;
        }
        synchronized (lock) {
            if (done) {
                return false;
            }
            cancelled = optionalCancellation.cancel();
            if (cancelled) {
                done = true;
                notifyChanges();
            }
        }
        return cancelled;
    }



    public final T get() throws InterruptedException, DbException {
        if (done) {
            return getResult();
        }
        synchronized (lock) {
            if (done) {
                return getResult();
            }
            while (!done) {
                lock.wait();
            }
        }
        return getResult();
    }

    public final T get(long timeout, TimeUnit unit) throws InterruptedException, DbException, TimeoutException {
        long timeoutMillis = unit.toMillis(timeout);
        long timeoutNanos = unit.toNanos(timeout);

        if (done) {
            return getResult();
        }
        synchronized (lock) {
            final long startTime = System.nanoTime();
            while (!done &&(startTime+timeoutNanos) > System.nanoTime() ){
                lock.wait(timeoutMillis);
            }
            if (!done) {
                throw new TimeoutException();
            }
        }
        return getResult();
    }

    private final T getResult() throws DbException {
        if (!done) {
            throw new IllegalStateException("Should not be calling this method when future is not done");
        }
        if (exception != null) {
            throw new DbException(exception);
        }
        if (cancelled) {
            throw new CancellationException();
        }
        return result;
    }

    public final void setResult(T result) {
        if (!trySetResult(result)) {
            throw new IllegalStateException("Should not set result if future is completed");
        }
    }

    public boolean trySetResult(T result) {
        synchronized (lock) {
            if (done) {
                return false;
            }

            this.result = result;
            done = true;
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
        return cancelled;
    }

    public boolean isDone() {
        return done;
    }

    public void setException(Throwable exception) {
        synchronized (lock) {
            if (done) {
                throw new IllegalStateException("Can't set exception on completed future");
            }
            this.exception = exception;
            done = true;
        }
        notifyChanges();
    }

    public <TResult> DbFuture<TResult> map(final OneArgFunction<T,TResult> transformation){
        final DefaultDbFuture<TResult> completion = new DefaultDbFuture<TResult>(delegateCancel());
        this.addListener(createTransformationListener(transformation, completion));
        return completion;

    }


    protected CancellationAction delegateCancel() {
        if(null==optionalCancellation){
            return null;
        }
        return new CancellationAction() {
            @Override
            public boolean cancel() {
                return DefaultDbFuture.this.cancel(true);
            }
        };
    }

    protected  <TResult> DbListener<T> createTransformationListener(final OneArgFunction<T, TResult> transformation,
                                                                 final DefaultDbFuture<TResult> completion) {
        return new DbListener<T>() {
            @Override
            public void onCompletion(DbFuture<T> future) {
                if(DefaultDbFuture.this.exception !=null){
                    completion.setException(DefaultDbFuture.this.exception);
                } else if(DefaultDbFuture.this.cancelled){
                    completion.forceToCanceledState();
                } else{
                    completion.setResult(transformation.apply(DefaultDbFuture.this.result));
                }
            }
        };
    }

    private void forceToCanceledState() {
        synchronized (this.lock){
            done=true;
            cancelled=true;
            notifyChanges();
        }
    }

}
