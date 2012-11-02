package org.adbcj.connectionpool;

import junit.framework.Assert;
import org.adbcj.*;
import org.adbcj.support.DefaultDbSessionFuture;
import org.adbcj.support.DefaultResultEventsHandler;
import org.adbcj.support.DefaultResultSet;

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
    public DbSessionFuture<Void> commit() {
        if(failTxOperation){
            return DefaultDbSessionFuture.createCompletedErrorFuture(this, new DbException("Failed"));
        } else{
            return DefaultDbSessionFuture.createCompletedFuture(this, null);
        }
    }

    @Override
    public DbSessionFuture<Void> rollback() {
        currentTxState = TransactionState.ROLLED_BACK;
        if(failTxOperation){
            return DefaultDbSessionFuture.createCompletedErrorFuture(this, new DbException("Failed"));
        } else{
            return DefaultDbSessionFuture.createCompletedFuture(this, null);
        }
    }

    @Override
    public boolean isInTransaction() {
        return this.currentTxState == TransactionState.ACTIVE;
    }

    @Override
    public DbSessionFuture<ResultSet> executeQuery(String sql) {
        return (DbSessionFuture) executeQuery(sql,new DefaultResultEventsHandler(),new DefaultResultSet());
    }

    @Override
    public <T> DbSessionFuture<T> executeQuery(String sql, ResultHandler<T> eventHandler, T accumulator) {
        return maySuccedingOperation(sql);
    }
    @Override
    public DbSessionFuture<Result> executeUpdate(String sql) {
         return maySuccedingOperation(sql);
    }

    @Override
    public DbSessionFuture<PreparedQuery> prepareQuery(String sql) {
        if(sql.equals(FAIL_QUERY)){
            return DefaultDbSessionFuture.createCompletedErrorFuture(this, new DbException("Failed: " + sql));
        }
        openStatements.incrementAndGet();
        return (DbSessionFuture) DefaultDbSessionFuture.createCompletedFuture(this, new MockPreparedQuery(sql,this));
    }

    @Override
    public DbSessionFuture<PreparedUpdate> prepareUpdate(String sql) {
        if(sql.equals(FAIL_QUERY)){
            return DefaultDbSessionFuture.createCompletedErrorFuture(this, new DbException("Failed: " + sql));
        }
        openStatements.incrementAndGet();
        return (DbSessionFuture) DefaultDbSessionFuture.createCompletedFuture(this, new MockPreparedUpdate(sql,this));
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
            return DefaultDbSessionFuture.createCompletedErrorFuture(this, new DbException("Failed"));
        } else{
            return DefaultDbSessionFuture.createCompletedFuture(this, null);
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

    private <T> DbSessionFuture<T> maySuccedingOperation(String sql) {
        if(sql.equals(FAIL_QUERY)){
            return DefaultDbSessionFuture.createCompletedErrorFuture(this, new DbException("Failed: " + sql));
        } else{
            return DefaultDbSessionFuture.createCompletedFuture(this,null);
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
