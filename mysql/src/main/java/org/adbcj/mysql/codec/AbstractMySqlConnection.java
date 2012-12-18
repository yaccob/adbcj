package org.adbcj.mysql.codec;

import org.adbcj.*;
import org.adbcj.mysql.netty.MysqlConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMySqlConnection implements Connection {

    private static final Logger logger = LoggerFactory.getLogger(AbstractMySqlConnection.class);

    private final MysqlConnectionManager connectionManager;

    protected final int id;

    private final MysqlCharacterSet charset = MysqlCharacterSet.UTF8_UNICODE_CI;

    protected AbstractMySqlConnection(MysqlConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        this.id = connectionManager.nextId();
        connectionManager.addConnection(this);
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public synchronized DbFuture<Void> close() throws DbException {
        return close(CloseMode.CLOSE_GRACEFULLY);
    }


    @Override
    public void beginTransaction() {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public DbSessionFuture<Void> commit() {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public DbSessionFuture<Void> rollback() {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public boolean isInTransaction() {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public DbSessionFuture<ResultSet> executeQuery(String sql) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public <T> DbSessionFuture<T> executeQuery(String sql, ResultHandler<T> eventHandler, T accumulator) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public DbSessionFuture<Result> executeUpdate(String sql) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public DbSessionFuture<PreparedQuery> prepareQuery(String sql) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public DbSessionFuture<PreparedUpdate> prepareUpdate(String sql) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public DbFuture<Void> close(CloseMode closeMode) throws DbException {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public boolean isClosed() throws DbException {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public boolean isOpen() throws DbException {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }
}
