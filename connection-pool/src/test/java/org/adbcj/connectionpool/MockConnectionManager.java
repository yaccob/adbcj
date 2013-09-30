package org.adbcj.connectionpool;

import junit.framework.Assert;
import org.adbcj.*;
import org.adbcj.support.DefaultDbFuture;

import java.util.concurrent.atomic.AtomicInteger;

/**
* @author roman.stoffel@gamlor.info
*/
class MockConnectionManager implements ConnectionManager {
    private final AtomicInteger connectionCounter = new AtomicInteger(0);
    private final AtomicInteger maxUsedConnections = new AtomicInteger(0);
    private volatile boolean closed = false;
    private final ThreadLocal<MockConnection> lastConnection = new ThreadLocal<MockConnection>();
    @Override
    public DbFuture<Connection> connect() {
        return connect("","");
    }

    @Override
    public DbFuture<Connection> connect(String user, String password) {
        final MockConnection connection = new MockConnection(this);
        lastConnection.set(connection);
        return DefaultDbFuture.<Connection>completed(connection);
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
        final int size = connectionCounter.incrementAndGet();
        int maxSize = maxUsedConnections.get();
        while(maxSize <size && maxUsedConnections.compareAndSet(maxSize,size)){
            maxSize = maxUsedConnections.get();
        }
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

    public MockConnection lastInstanceRequestedOnThisThread() {
        return lastConnection.get();
    }

    public void assertMaxConnectonsUsed(int expectedMaxSize) {
        Assert.assertEquals("Expect the amount of max open connections",expectedMaxSize,maxUsedConnections.get());
    }
}

