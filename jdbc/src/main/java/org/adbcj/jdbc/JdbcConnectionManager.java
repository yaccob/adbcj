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
import org.adbcj.support.UncheckedThrow;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class JdbcConnectionManager implements ConnectionManager {
	private final ExecutorService executorService;

	private final Object lock = this;
    private final JDBCConnectionProvider connectionProvider;
	private final Set<JdbcConnection> connections = new HashSet<JdbcConnection>(); // Access must be synchronized on lock
	
	private volatile DefaultDbFuture<Void> closeFuture;

	private volatile boolean pipeliningEnabled = false;

	public JdbcConnectionManager(JDBCConnectionProvider connectionProvider) {
		this(createPool(), connectionProvider);
	}

    public JdbcConnectionManager(ExecutorService executorService, JDBCConnectionProvider connectionProvider) {
        this.executorService = executorService;
        this.connectionProvider = connectionProvider;
    }

    public DbFuture<Connection> connect() throws DbException {
		if (isClosed()) {
			throw new DbException("This connection manager is closed");
		}
		final DefaultDbFuture<Connection> future = new DefaultDbFuture<Connection>();
		executorService.submit(new Callable<Connection>() {
			public Connection call() throws Exception {
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
					return connection;
				} catch (Exception e) {
					future.setException(new DbException(e));
					throw e;
				} catch (Throwable e) {
					future.setException(new DbException(e));
                    // We throw the original exception here.
                    // No nesting is used.
                    throw UncheckedThrow.throwUnchecked(e);
				}
			}
		});
		return future;
	}

	public DbFuture<Void> close() throws DbException {
		synchronized (lock) {
			if (closeFuture == null) {
				closeFuture = new DefaultDbFuture<Void>();
				closeFuture.addListener(new DbListener<Void>() {
					@Override
					public void onCompletion(DbFuture<Void> future) throws Exception {
						executorService.shutdown();
					}
				});
			} else {
				return closeFuture;
			}
		}
		final AtomicInteger countDown = new AtomicInteger();
		final AtomicBoolean allClosed = new AtomicBoolean(false);
		
		DbListener<Void> listener = new DbListener<Void>() {
			@Override
			public void onCompletion(DbFuture<Void> future) {
				try {
					int count = countDown.decrementAndGet();
					future.get();
					if (allClosed.get() && count == 0) {
						closeFuture.setResult(null);
					}
				} catch (Exception e) {
					// If the connection close errored out, error out our closeFuture too
					closeFuture.setException(e);
				}
			}
		};
		synchronized (lock) {
			for (JdbcConnection connection : connections) {
				countDown.incrementAndGet();
				connection.close().addListener(listener);
			}
		}
		allClosed.set(true);
		if (countDown.get() == 0) {
			closeFuture.setResult(null);
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
