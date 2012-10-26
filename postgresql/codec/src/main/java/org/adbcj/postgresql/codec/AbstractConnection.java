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
package org.adbcj.postgresql.codec;

import org.adbcj.*;
import org.adbcj.postgresql.codec.frontend.*;
import org.adbcj.support.AbstractDbSession;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.ExpectResultRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractConnection extends AbstractDbSession implements Connection {

	private final Logger logger = LoggerFactory.getLogger(AbstractConnection.class);

	private final AbstractConnectionManager connectionManager;
	private final ConnectionState connectionState;
	private volatile Request<Void> closeRequest;

	private volatile int pid;
	private volatile int key;

	// Constant Messages
	private static final ExecuteMessage DEFAULT_EXECUTE = new ExecuteMessage();
	private static final BindMessage DEFAULT_BIND = new BindMessage();
	private static final DescribeMessage DEFAULT_DESCRIBE = DescribeMessage.createDescribePortalMessage(null);

	public AbstractConnection(AbstractConnectionManager connectionManager) {
		super();
		this.connectionManager = connectionManager;
		this.connectionState = new ConnectionState(connectionManager.getDatabase());
	}

	public AbstractConnectionManager getConnectionManager() {
		return connectionManager;
	}

	@Override
	protected void checkClosed() {
		if (isClosed()) {
			throw new DbSessionClosedException( "This connection has been closed");
		}
	}

	public DbFuture<Void> close() throws DbException {

		// If the connection is already closed, return existing finalizeClose future
		synchronized (lock) {
			if (isClosed()) {
				return DefaultDbFuture.completed(null);
			} if(null!=closeRequest){
                return closeRequest.getFuture();
            } else {
					// If the finalizeClose is NOT immediate, schedule the finalizeClose
                this.closeRequest =  new Request<Void>(this) {
						@Override
						public boolean cancelRequest(boolean mayInterruptIfRunning) {
							return false;
						}
						@Override
						public void execute() {
							logger.debug("Sending TERMINATE to server (Request queue size: {})", requestQueue.size());
							write(SimpleFrontendMessage.TERMINATE);
						}
						@Override
						public boolean isPipelinable() {
							return false;
						}
						@Override
						public String toString() {
							return "close request";
						}
					};
					return enqueueRequest(closeRequest).getFuture();
			}
		}
	}

	public boolean isClosed() throws DbException {
		synchronized (lock) {
			return isConnectionClosing();
		}
	}

    @Override
    public boolean isOpen() throws DbException {
        return !isClosed();
    }

    void finalizeClose() throws DbException {
		// TODO Make a DbSessionClosedException and use here
		errorPendingRequests(new DbException("Connection closed"));
		synchronized (lock) {
			if (closeRequest != null) {
				closeRequest.complete(null);
			}
		}
	}

	public <T> DbSessionFuture<T> executeQuery(final String sql, ResultHandler<T> eventHandler, T accumulator) {
		checkClosed();
		Request<T> request = new ExpectResultRequest<T>(this,eventHandler, accumulator) {
			@Override
			public void execute() throws Exception {
				logger.debug("Issuing query: {}", sql);

				ParseMessage parse = new ParseMessage(sql);
				write(new AbstractFrontendMessage[] {
					parse,
					DEFAULT_BIND,
					DEFAULT_DESCRIBE,
					DEFAULT_EXECUTE,
					SimpleFrontendMessage.SYNC,
				});
			}
			@Override
			public String toString() {
				return "SELECT request: " + sql;
			}
		};
		return enqueueTransactionalRequest(request);
	}

	public DbSessionFuture<Result> executeUpdate(final String sql) {
		checkClosed();
		return enqueueTransactionalRequest(new Request<Result>(this) {
			@Override
			public void execute() throws Exception {
				logger.debug("Issuing update query: {}", sql);

				ParseMessage parse = new ParseMessage(sql);
				write(new AbstractFrontendMessage[] {
					parse,
					DEFAULT_BIND,
					DEFAULT_DESCRIBE,
					DEFAULT_EXECUTE,
					SimpleFrontendMessage.SYNC
				});
			}

			@Override
			public String toString() {
				return "Update request: " + sql;
			}
		});
	}

    @Override
    public DbSessionFuture<PreparedQuery> prepareQuery(String sql) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public DbSessionFuture<PreparedUpdate> prepareUpdate(String sql) {
        throw new UnsupportedOperationException("Not implemented");
    }

    // ******** Transaction methods ***********************************************************************************

	private final AtomicLong statementCounter = new AtomicLong();
	private final Map<String, String> statementCache = Collections.synchronizedMap(new HashMap<String, String>());

	@Override
	protected void sendBegin() {
		executeStatement("BEGIN");
	}

	@Override
	protected void sendCommit() {
		executeStatement("COMMIT");
	}

	@Override
	protected void sendRollback() {
		executeStatement("ROLLBACK");
	}

	private void executeStatement(String statement) {
		String statementId = statementCache.get(statement);
		if (statementId == null) {
			long id = statementCounter.incrementAndGet();
			statementId = "S_" + id;

			ParseMessage parseMessage = new ParseMessage(statement, statementId);
			write(parseMessage);

			statementCache.put(statement, statementId);
		}
		write(new AbstractFrontendMessage[] {
				new BindMessage(statementId),
				DEFAULT_EXECUTE,
				SimpleFrontendMessage.SYNC
		});
	}

	// ================================================================================================================
	//
	// Non-API methods
	//
	// ================================================================================================================

	@Override
	protected <E> Request<E> enqueueRequest(Request<E> request) {
		return super.enqueueRequest(request);
	}

	@Override
	public <E> Request<E> getActiveRequest() {
		return super.getActiveRequest();
	}

	public int getPid() {
		return pid;
	}

	public void setPid(int pid) {
		this.pid = pid;
	}

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	public ConnectionState getConnectionState() {
		return connectionState;
	}

	// ================================================================================================================
	//
	// Abstract methods to be implemented by transport specific implementations
	//
	// ================================================================================================================

	protected abstract void write(AbstractFrontendMessage message);

	protected abstract void write(AbstractFrontendMessage[] messages);

	protected abstract boolean isConnectionClosing();

	public abstract DefaultDbFuture<Connection> getConnectFuture();

}
