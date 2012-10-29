package org.adbcj.connectionpool;

import org.adbcj.*;

class AbstractMockPreparedStatement {


    public <T> DbSessionFuture<T> executeWithCallback(ResultHandler<T> eventHandler, T accumulator, Object... params) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    public boolean isClosed() {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    public DbFuture<Void> close() {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }
}
class MockPreparedQuery extends AbstractMockPreparedStatement implements PreparedQuery{
    public MockPreparedQuery(String sql) {
    }

    @Override
    public DbFuture<ResultSet> execute(Object... params) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }
}

class MockPreparedUpdate extends AbstractMockPreparedStatement implements PreparedUpdate{
    public MockPreparedUpdate(String sql) {
    }

    @Override
    public DbFuture<Result> execute(Object... params) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }
}
