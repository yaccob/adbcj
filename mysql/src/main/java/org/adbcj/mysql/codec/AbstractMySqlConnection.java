package org.adbcj.mysql.codec;

import org.adbcj.*;
import org.adbcj.mysql.codec.packets.Command;
import org.adbcj.mysql.codec.packets.CommandRequest;
import org.adbcj.mysql.codec.packets.StringCommandRequest;
import org.adbcj.support.AbstractDbSession;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.ExpectResultRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.Set;

public abstract class AbstractMySqlConnection extends AbstractDbSession implements Connection {

	private static final Logger logger = LoggerFactory.getLogger(AbstractMySqlConnection.class);

	private final AbstractMySqlConnectionManager connectionManager;

	private final int id;

	private final LoginCredentials credentials;

	private volatile Request<Void> closeRequest;

	private final MysqlCharacterSet charset = MysqlCharacterSet.UTF8_UNICODE_CI;

	protected AbstractMySqlConnection(AbstractMySqlConnectionManager connectionManager, LoginCredentials credentials) {
		super();
		this.connectionManager = connectionManager;
		this.credentials = credentials;
		this.id = connectionManager.nextId();
		connectionManager.addConnection(this);
	}

	public abstract void write(ClientRequest request);

	protected abstract boolean isTransportClosing();

	public abstract DefaultDbFuture<Connection> getConnectFuture();

	public ConnectionManager getConnectionManager() {
		return connectionManager;
	}

	public synchronized DbFuture<Void> close() throws DbException {
		// If the connection is already closed, return existing close future
		logger.debug("Closing");
		if (isClosed()) {
			return DefaultDbFuture.completed(null);
		} if(closeRequest!=null){
            return closeRequest;
        }else {
            closeRequest = new CloseRequest();
            enqueueRequest(closeRequest);
		}
		logger.trace("Exiting close()");
		return closeRequest;
	}

	public synchronized boolean isClosed() {
		return closeRequest != null || isTransportClosing();
	}
	public synchronized boolean isOpen() {
		return !isClosed();
	}

	public <T> DbSessionFuture<T> executeQuery(final String sql, ResultHandler<T> eventHandler, T accumulator) {
		checkClosed();
		return enqueueTransactionalRequest(new ExpectResultRequest<T>(this,eventHandler, accumulator) {
			@Override
			public void execute() throws Exception {
				logger.debug("Sending query '{}'", sql);
				CommandRequest request = new StringCommandRequest(Command.QUERY, sql);
				write(request);
        }
			@Override
			public String toString() {
				return "SELECT request: " + sql;
			}
		});
	}

	public DbSessionFuture<Result> executeUpdate(final String sql) {
		checkClosed();
		logger.debug("Scheduling update '{}'", sql);
		return enqueueTransactionalRequest(new Request<Result>(this) {
			public void execute() {
				logger.debug("Sending update '{}'", sql);
				CommandRequest request = new StringCommandRequest(Command.QUERY, sql);
				write(request);
			}
			@Override
			public String toString() {
				return "MySQL update: " + sql;
			}
		});
	}

	public DbSessionFuture<PreparedQuery> prepareQuery(final String sql) {
		checkClosed();
        return enqueueTransactionalRequest(new PreparedStatementRequest(sql));
	}
	public DbSessionFuture<PreparedUpdate> prepareUpdate(final String sql) {
		checkClosed();
        return enqueueTransactionalRequest(new PreparedStatementRequest(sql));
	}


	// ************* Transaction method implementations ******************************************

	private static final CommandRequest BEGIN = new StringCommandRequest(Command.QUERY, "begin");
	private static final CommandRequest COMMIT = new StringCommandRequest(Command.QUERY, "commit");
	private static final CommandRequest ROLLBACK = new StringCommandRequest(Command.QUERY, "rollback");

	@Override
	protected void sendCommit() {
		write(COMMIT);
	}

	@Override
	protected void sendRollback() {
		write(ROLLBACK);
	}

	@Override
	protected void sendBegin() {
		write(BEGIN);
	}

	// ************* Non-API methods *************************************************************

	public LoginCredentials getCredentials() {
		return credentials;
	}

	public MysqlCharacterSet getCharacterSet() {
		return charset;
	}

	private static final Set<ClientCapabilities> CLIENT_CAPABILITIES = EnumSet.of(
			ClientCapabilities.LONG_PASSWORD,
			ClientCapabilities.FOUND_ROWS,
			ClientCapabilities.LONG_COLUMN_FLAG,
			ClientCapabilities.CONNECT_WITH_DB,
			ClientCapabilities.LOCAL_FILES,
			ClientCapabilities.PROTOCOL_4_1,
			ClientCapabilities.TRANSACTIONS,
			ClientCapabilities.SECURE_AUTHENTICATION);

	public Set<ClientCapabilities> getClientCapabilities() {
		return CLIENT_CAPABILITIES;
	}

	private static final Set<ExtendedClientCapabilities> EXTENDED_CLIENT_CAPABILITIES = EnumSet.of(
			ExtendedClientCapabilities.MULTI_RESULTS
			);

	public Set<ExtendedClientCapabilities> getExtendedClientCapabilities() {
		return EXTENDED_CLIENT_CAPABILITIES;
	}

	protected void checkClosed() {
		if (isClosed()) {
			throw new DbSessionClosedException("This connection has been closed");
		}
	}

	//
	//
	// Queuing methods
	////

	/*
	 * Make this method public.
	 */
	@Override
	public <E> Request<E> enqueueRequest(Request<E> request) {
		return super.enqueueRequest(request);
	}

	/*
	 * Make this method public
	 */
	@Override
	public <E> Request<E> getActiveRequest() {
		return super.getActiveRequest();
	}

	@Override
	public int hashCode() {
		return id;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (this == obj) {
			return true;
		}
		if (!(obj.getClass() == getClass())) {
			return false;
		}
		return id == ((AbstractMySqlConnection)obj).id;
	}

	public void doClose() {
		connectionManager.removeConnection(this);

		// TODO Make a DbSessionClosedException and use here
		DbException closedException = new DbException("Connection closed");
		if (!getConnectFuture().isDone() ) {
			getConnectFuture().setException(closedException);
		}
        Request<Object> closeRequestAsObject = (Request)closeRequest;
        if (getActiveRequest() != closeRequestAsObject) {
            errorPendingRequests(closedException);
        }
		synchronized (this) {
			if (closeRequest != null) {
				closeRequest.tryComplete(null);
			}
		}
	}

    @Override
    public <E> DbSessionFuture<E> enqueueTransactionalRequest(Request<E> request) {
        return super.enqueueTransactionalRequest(request);}

    private class CloseRequest extends Request<Void>{
        private CloseRequest() {
            super(AbstractMySqlConnection.this);
        }

        @Override
        public boolean cancelRequest(boolean mayInterruptIfRunning) {
            return false;
        }
        public synchronized void execute() {
            logger.debug("Sending QUIT to server");
            write(new CommandRequest(Command.QUIT));
        }
        @Override
        public String toString() {
            return "MySQL deferred close";
        }
    }

    public class PreparedStatementRequest<T extends PreparedStatement> extends Request<T> {
        private final String sql;

        public PreparedStatementRequest(String sql) {
            super(AbstractMySqlConnection.this);
            this.sql = sql;
        }

        @Override
        public void execute() throws Exception {
            logger.debug("Sending prepared query '{}'", sql);
            CommandRequest request = new StringCommandRequest(Command.STATEMENT_PREPARE, sql);
            write(request);
        }

        @Override
        public String toString() {
            return "SELECT PREPARE request: " + sql;
        }


    }
}
