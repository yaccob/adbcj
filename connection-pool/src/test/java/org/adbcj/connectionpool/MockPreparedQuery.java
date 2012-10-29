package org.adbcj.connectionpool;

import org.adbcj.*;
import org.adbcj.support.DefaultDbFuture;

class AbstractMockPreparedStatement {
    private final MockConnection connection;

    AbstractMockPreparedStatement(MockConnection connection) {
        this.connection = connection;
    }

    public <T> DbSessionFuture<T> executeWithCallback(ResultHandler<T> eventHandler, T accumulator, Object... params) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    public boolean isClosed() {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    public DbFuture<Void> close() {
        connection.openStatements.decrementAndGet();
        return DefaultDbFuture.completed(null);
    }
}
class MockPreparedQuery extends AbstractMockPreparedStatement implements PreparedQuery{
    public MockPreparedQuery(String sql, MockConnection connection) {
        super(connection);
    }

    @Override
    public DbFuture<ResultSet> execute(Object... params) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }
}

class MockPreparedUpdate extends AbstractMockPreparedStatement implements PreparedUpdate{
    public MockPreparedUpdate(String sql, MockConnection connection) {
        super(connection);
    }

    @Override
    public DbFuture<Result> execute(Object... params) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }
}
