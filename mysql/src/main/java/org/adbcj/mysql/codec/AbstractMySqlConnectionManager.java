package org.adbcj.mysql.codec;

import org.adbcj.*;
import org.adbcj.support.DefaultDbFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractMySqlConnectionManager implements
		ConnectionManager {
	private static final Logger logger = LoggerFactory.getLogger(AbstractMySqlConnectionManager.class);

	private final LoginCredentials credentials;

	private final AtomicInteger id = new AtomicInteger();
	private final Set<AbstractMySqlConnection> connections = new HashSet<AbstractMySqlConnection>();

	private DbFuture<Void> closeFuture = null;

	public AbstractMySqlConnectionManager(String username, String password, String schema) {
		this.credentials = new LoginCredentials(username, password, schema);
	}

    public DbFuture<Void> close(){
        return close(CloseMode.CLOSE_GRACEFULLY);
    }

	public DbFuture<Void> close(CloseMode closeMode) throws DbException {
		if (isClosed()) {
			return closeFuture;
		}
        ArrayList<AbstractMySqlConnection> connectionsCopy;
        synchronized (connections) {
            connectionsCopy = new ArrayList<AbstractMySqlConnection>(connections);
        }
        for (AbstractMySqlConnection connection : connectionsCopy) {
            connection.close(closeMode);
        }
        dispose();
        DefaultDbFuture<Void> future = new DefaultDbFuture<Void>();
        future.setResult(null);
        closeFuture = future;
        return closeFuture;
	}

	public synchronized boolean isClosed() {
		return closeFuture != null;
	}

	public DbFuture<Connection> connect() {
		if (isClosed()) {
			throw new DbException("Connection manager closed");
		}
		logger.debug("Starting connection");

		return createConnectionFuture();
	}

	protected abstract void dispose();

	protected abstract DefaultDbFuture<Connection> createConnectionFuture();



	protected void addConnection(AbstractMySqlConnection connection) {
		synchronized (connections) {
			connections.add(connection);
		}
	}

	protected boolean removeConnection(AbstractMySqlConnection connection) {
		synchronized (connections) {
			return connections.remove(connection);
		}
	}

	int nextId() {
		return id.incrementAndGet();
	}

	protected LoginCredentials getCredentials() {
		return credentials;
	}

	@Override
	public String toString() {
		return getClass().getName() + ": " + credentials.getDatabase();
	}
}
