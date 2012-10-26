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
import org.adbcj.support.DefaultDbFuture;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class JdbcConnectionManager implements ConnectionManager {
	private final ExecutorService executorService;

	private final Object lock = this;
    private final JDBCConnectionProvider connectionProvider;
	private final Set<JdbcConnection> connections = new HashSet<JdbcConnection>(); // Access must be synchronized on lock
	
	private volatile DefaultDbFuture<Void> closeFuture;

	private volatile boolean pipeliningEnabled = false;


    public JdbcConnectionManager(ExecutorService executorService,
                                 JDBCConnectionProvider connectionProvider) {
        this.executorService = executorService;
        this.connectionProvider = connectionProvider;
    }

    public DbFuture<Connection> connect() throws DbException {
		if (isClosed()) {
			throw new DbException("This connection manager is closed");
		}
		final DefaultDbFuture<Connection> future = new DefaultDbFuture<Connection>();
		executorService.execute(new Runnable() {
			public void run() {
				try {
					java.sql.Connection jdbcConnection = connectionProvider.getConnection();
					JdbcConnection connection = new JdbcConnection(JdbcConnectionManager.this, jdbcConnection,getExecutorService());
					synchronized (lock) {
						if (isClosed()) {
							connection.close();
							future.setException(new DbException("Connection manager closed"));
						} else {
							connections.add(connection);
							future.setResult(connection);
						}
					}
				} catch (Throwable e) {
                    DbException ex = DbException.wrap(e);
					future.setException(ex);
                    throw ex;
				}
			}
		});
		return future;
	}

	public DbFuture<Void> close() throws DbException {
        return close(CloseMode.CLOSE_GRACEFULLY);
	}

    @Override
    public DbFuture<Void> close(CloseMode mode) throws DbException {
        synchronized (lock) {
            if (closeFuture == null) {
                closeFuture = new DefaultDbFuture<Void>();
                closeFuture.addListener(new DbListener<Void>() {
                    @Override
                    public void onCompletion(DbFuture<Void> future) {
                        executorService.shutdown();
                    }
                });
            } else {
                return closeFuture;
            }
        }

        synchronized (lock) {
            final AtomicInteger latch = new AtomicInteger(connections.size());
            for (JdbcConnection connection : connections) {
                connection.close(mode).addListener(new DbListener<Void>() {
                    @Override
                    public void onCompletion(DbFuture<Void> future) {
                        final int currentCount = latch.decrementAndGet();
                        if(currentCount==0){
                            closeFuture.trySetResult(null);
                        }
                    }
                });
            }
        }
        return closeFuture;
    }

    public boolean isClosed() {
		return closeFuture != null;
	}
	
	public ExecutorService getExecutorService() {
		return executorService;
	}

	boolean removeConnection(JdbcConnection connection) {
		synchronized (lock) {
			return connections.remove(connection);
		}
	}

	public boolean isPipeliningEnabled() {
		return pipeliningEnabled;
	}

	public void setPipeliningEnabled(boolean pipeliningEnabled) {
		this.pipeliningEnabled = pipeliningEnabled;
	}

	@Override
	public String toString() {
		return "JdbcConnectionManager with" + connectionProvider.toString();
	}

    private static ExecutorService createPool() {
        ExecutorService executorService = new ThreadPoolExecutor(0, 64,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                new ThreadFactory() {
                    private final AtomicInteger threadNumber = new AtomicInteger(1);

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = Executors.defaultThreadFactory().newThread(r);
                        thread.setName("ADBC to JDBC bridge " + threadNumber.incrementAndGet());
                        thread.setDaemon(true);
                        return thread;
                    }
                });
        return executorService;
    }
	
}
