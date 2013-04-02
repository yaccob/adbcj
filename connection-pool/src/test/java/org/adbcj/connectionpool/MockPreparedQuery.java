package org.adbcj.connectionpool;

import org.adbcj.*;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.stacktracing.StackTracingOptions;

class AbstractMockPreparedStatement {
    private final String sql;
    private final MockConnection connection;
    public static final String FAIL_STATEMENT_EXECUTE = "fail-prepared-statement";

    AbstractMockPreparedStatement(String sql,MockConnection connection) {
        this.sql = sql;
        this.connection = connection;
    }


    public boolean isClosed() {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    public DbFuture<Void> close() {
        connection.openStatements.decrementAndGet();
        return DefaultDbFuture.completed(null);
    }

    DbFuture executeOrFail(){

        if(sql.equals(FAIL_STATEMENT_EXECUTE)){
            return DefaultDbFuture.createCompletedErrorFuture(StackTracingOptions.FORCED_BY_INSTANCE, new DbException("Failed: " + sql));
        }else {
            return DefaultDbFuture.completed(null);
        }
    }
}
class MockPreparedQuery extends AbstractMockPreparedStatement implements PreparedQuery{
    public MockPreparedQuery(String sql, MockConnection connection) {
        super(sql,connection);
    }

    @Override
    public DbFuture<ResultSet> execute(Object... params) {
        return executeOrFail();
    }
    public <T> DbFuture<T> executeWithCallback(ResultHandler<T> eventHandler, T accumulator, Object... params) {
        return executeOrFail();
    }
}

class MockPreparedUpdate extends AbstractMockPreparedStatement implements PreparedUpdate{
    public MockPreparedUpdate(String sql, MockConnection connection) {
        super(sql,connection);
    }

    @Override
    public DbFuture<Result> execute(Object... params) {
        return executeOrFail();
    }
}
