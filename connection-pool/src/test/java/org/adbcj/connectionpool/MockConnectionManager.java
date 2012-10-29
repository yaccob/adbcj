package org.adbcj.connectionpool;

import junit.framework.Assert;
import org.adbcj.*;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.DefaultDbSessionFuture;

import java.util.concurrent.atomic.AtomicInteger;

/**
* @author roman.stoffel@gamlor.info
*/
class MockConnectionManager implements ConnectionManager {
    private final AtomicInteger connectionCounter = new AtomicInteger(0);
    private volatile boolean closed = false;
    @Override
    public DbFuture<Connection> connect() {
        return DefaultDbFuture.<Connection>completed(new MockConnection(this));
    }

    @Override
    public DbFuture<Void> close() throws DbException {
        return close(CloseMode.CLOSE_GRACEFULLY);
    }

    @Override
    public DbFuture<Void> close(CloseMode mode) throws DbException {
        closed = true;
        return DefaultDbFuture.completed(null);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }


    void incrementConnectionCounter() {
        connectionCounter.incrementAndGet();
    }

    void decementConnectionCounter() {
        connectionCounter.decrementAndGet();
    }

    public void assertWasClosed() {
        Assert.assertTrue("Expected that the manager has been closed",closed);
    }

    public void assertConnectionAlive(int expectedAmount) {

        Assert.assertEquals("Expect the amount of open connections",expectedAmount,connectionCounter.get());
    }
}

class MockConnection implements Connection{

    private MockConnectionManager connection;

    public MockConnection(MockConnectionManager mockConnectionManager) {
        this.connection = mockConnectionManager;
        mockConnectionManager.incrementConnectionCounter();
    }

    @Override
    public ConnectionManager getConnectionManager() {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
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
        return (DbSessionFuture) DefaultDbSessionFuture.createCompletedFuture(this, new MockPreparedQuery(sql));
    }

    @Override
    public DbSessionFuture<PreparedUpdate> prepareUpdate(String sql) {
        return (DbSessionFuture) DefaultDbSessionFuture.createCompletedFuture(this, new MockPreparedUpdate(sql));
    }

    @Override
    public DbFuture<Void> close() throws DbException {
        return close(CloseMode.CLOSE_GRACEFULLY);
    }

    @Override
    public DbFuture<Void> close(CloseMode closeMode) throws DbException {
        connection.decementConnectionCounter();
        return DefaultDbFuture.completed(null);
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
