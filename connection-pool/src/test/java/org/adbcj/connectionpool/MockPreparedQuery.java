package org.adbcj.connectionpool;

import org.adbcj.*;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.DefaultDbSessionFuture;

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

    DbSessionFuture executeOrFail(){

        if(sql.equals(FAIL_STATEMENT_EXECUTE)){
            return DefaultDbSessionFuture.createCompletedErrorFuture(connection, new DbException("Failed: " + sql));
        }else {
            return DefaultDbSessionFuture.createCompletedFuture(connection,null);
        }
    }
}
class MockPreparedQuery extends AbstractMockPreparedStatement implements PreparedQuery{
    public MockPreparedQuery(String sql, MockConnection connection) {
        super(sql,connection);
    }

    @Override
    public DbSessionFuture<ResultSet> execute(Object... params) {
        return executeOrFail();
    }
    public <T> DbSessionFuture<T> executeWithCallback(ResultHandler<T> eventHandler, T accumulator, Object... params) {
        return executeOrFail();
    }
}

class MockPreparedUpdate extends AbstractMockPreparedStatement implements PreparedUpdate{
    public MockPreparedUpdate(String sql, MockConnection connection) {
        super(sql,connection);
    }

    @Override
    public DbSessionFuture<Result> execute(Object... params) {
        return executeOrFail();
    }
}
