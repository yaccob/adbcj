package org.adbcj.connectionpool;

import junit.framework.Assert;
import org.adbcj.*;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.DefaultResultEventsHandler;
import org.adbcj.support.DefaultResultSet;
import org.adbcj.support.stacktracing.StackTracingOptions;

import java.util.concurrent.atomic.AtomicInteger;

public class MockConnection implements Connection {

    public static final String FAIL_QUERY = "fail-query";
    private final MockConnectionManager connection;
    final AtomicInteger openStatements = new AtomicInteger();
    private volatile TransactionState currentTxState = TransactionState.NONE;
    private volatile boolean closed = false;
    private volatile boolean failTxOperation = false;
    private volatile boolean failCloseOperation = false;

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
        this.currentTxState = TransactionState.ACTIVE;
    }

    @Override
    public DbFuture<Void> commit() {
        if(failTxOperation){
            return DefaultDbFuture.createCompletedErrorFuture(StackTracingOptions.FORCED_BY_INSTANCE, new DbException("Failed"));
        } else{
            return DefaultDbFuture.completed(null);
        }
    }

    @Override
    public DbFuture<Void> rollback() {
        currentTxState = TransactionState.ROLLED_BACK;
        if(failTxOperation){
            return DefaultDbFuture.createCompletedErrorFuture(StackTracingOptions.FORCED_BY_INSTANCE, new DbException("Failed"));
        } else{
            return DefaultDbFuture.completed(null);
        }
    }

    @Override
    public boolean isInTransaction() {
        return this.currentTxState == TransactionState.ACTIVE;
    }

    @Override
    public DbFuture<ResultSet> executeQuery(String sql) {
        return (DbFuture) executeQuery(sql,new DefaultResultEventsHandler(),new DefaultResultSet());
    }

    @Override
    public <T> DbFuture<T> executeQuery(String sql, ResultHandler<T> eventHandler, T accumulator) {
        return maySuccedingOperation(sql);
    }
    @Override
    public DbFuture<Result> executeUpdate(String sql) {
         return maySuccedingOperation(sql);
    }

    @Override
    public DbFuture<PreparedQuery> prepareQuery(String sql) {
        if(sql.equals(FAIL_QUERY)){
            return DefaultDbFuture.createCompletedErrorFuture(StackTracingOptions.FORCED_BY_INSTANCE, new DbException("Failed: " + sql));
        }
        openStatements.incrementAndGet();
        return (DbFuture) DefaultDbFuture.completed(new MockPreparedQuery(sql,this));
    }

    @Override
    public DbFuture<PreparedUpdate> prepareUpdate(String sql) {
        if(sql.equals(FAIL_QUERY)){
            return DefaultDbFuture.createCompletedErrorFuture(StackTracingOptions.FORCED_BY_INSTANCE,  new DbException("Failed: " + sql));
        }
        openStatements.incrementAndGet();
        return (DbFuture) DefaultDbFuture.completed(new MockPreparedUpdate(sql,this));
    }

    @Override
    public DbFuture<Void> close() throws DbException {
        return close(CloseMode.CLOSE_GRACEFULLY);
    }

    @Override
    public DbFuture<Void> close(CloseMode closeMode) throws DbException {
        closed = true;
        connection.decementConnectionCounter();

        if(failCloseOperation){
            return DefaultDbFuture.createCompletedErrorFuture(StackTracingOptions.FORCED_BY_INSTANCE, new DbException("Failed"));
        } else{
            return DefaultDbFuture.completed(null);
        }
    }

    @Override
    public boolean isClosed() throws DbException {
        return closed;
    }

    @Override
    public boolean isOpen() throws DbException {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    private <T> DbFuture<T> maySuccedingOperation(String sql) {
        if(sql.equals(FAIL_QUERY)){
            return DefaultDbFuture.createCompletedErrorFuture(StackTracingOptions.FORCED_BY_INSTANCE, new DbException("Failed: " + sql));
        } else{
            return DefaultDbFuture.completed(null);
        }
    }


    public void assertAmountOfPreparedStatements(int expectedAmount) {
        Assert.assertEquals("Expect this amount of open statements", expectedAmount, openStatements.get());
    }

    public void assertTransactionState(TransactionState expectedState) {
        Assert.assertEquals(expectedState,currentTxState);
    }

    public void failTxOperation() {
        this.failTxOperation = true;
    }

    public void failCloseOperation() {
        this.failCloseOperation = true;
    }
}
