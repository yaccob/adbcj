package org.adbcj.connectionpool;

import org.adbcj.*;

/**
 * @author roman.stoffel@gamlor.info
 */
public class PooledConnection implements Connection {
    private final Connection nativeConnection;
    private final PooledConnectionManager pooledConnectionManager;

    public PooledConnection(Connection nativConnection, PooledConnectionManager pooledConnectionManager) {
        this.nativeConnection = nativConnection;
        this.pooledConnectionManager = pooledConnectionManager;
    }

    @Override
    public ConnectionManager getConnectionManager() {
        return nativeConnection.getConnectionManager();
    }

    @Override
    public void beginTransaction() {
        nativeConnection.beginTransaction();
    }

    @Override
    public DbSessionFuture<Void> commit() {
        return nativeConnection.commit();
    }

    @Override
    public DbSessionFuture<Void> rollback() {
        return nativeConnection.rollback();
    }

    @Override
    public boolean isInTransaction() {
        return nativeConnection.isInTransaction();
    }

    @Override
    public DbSessionFuture<ResultSet> executeQuery(String sql) {
        return nativeConnection.executeQuery(sql);
    }

    @Override
    public <T> DbSessionFuture<T> executeQuery(String sql, ResultHandler<T> eventHandler, T accumulator) {
        return nativeConnection.executeQuery(sql, eventHandler, accumulator);
    }

    @Override
    public DbSessionFuture<Result> executeUpdate(String sql) {
        return nativeConnection.executeUpdate(sql);
    }

    @Override
    public DbSessionFuture<PreparedQuery> prepareQuery(String sql) {
        return nativeConnection.prepareQuery(sql);
    }

    @Override
    public DbSessionFuture<PreparedUpdate> prepareUpdate(String sql) {
        return nativeConnection.prepareUpdate(sql);
    }

    @Override
    public DbFuture<Void> close() throws DbException {
        return close(CloseMode.CLOSE_GRACEFULLY);
    }

    @Override
    public DbFuture<Void> close(CloseMode closeMode) throws DbException {
        return pooledConnectionManager.returnConnection(this);
    }

    @Override
    public boolean isClosed() throws DbException {
        return nativeConnection.isClosed();
    }

    @Override
    public boolean isOpen() throws DbException {
        return nativeConnection.isOpen();
    }

    public Connection getNativeConnection() {
        return nativeConnection;
    }
}
