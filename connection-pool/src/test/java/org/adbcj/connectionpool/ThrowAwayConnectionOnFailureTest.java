package org.adbcj.connectionpool;

import junit.framework.Assert;
import org.adbcj.*;
import org.adbcj.support.DefaultResultEventsHandler;
import org.adbcj.support.DefaultResultSet;
import org.testng.annotations.Test;

import static org.adbcj.connectionpool.MockConnection.*;

/**
 * @author roman.stoffel@gamlor.info
 */
public class ThrowAwayConnectionOnFailureTest {


    @Test
    public void failureInSelectThrowsAwayConnection() throws InterruptedException {

        expectCloseAfterOperation(new ConnectionAction() {
            @Override
            public DbFuture invoke(Connection connection) {
                return connection.executeQuery("fail-query",new DefaultResultEventsHandler(), new DefaultResultSet());
            }
        });
        expectCloseAfterOperation(new ConnectionAction() {
            @Override
            public DbFuture invoke(Connection connection) {
                return connection.executeQuery(FAIL_QUERY);
            }
        });
    }
    @Test
    public void failureInUpdateThrowsAwayConnection() throws InterruptedException {

        expectCloseAfterOperation(new ConnectionAction() {
            @Override
            public DbFuture invoke(Connection connection) {
                return connection.executeUpdate(FAIL_QUERY);
            }
        });
    }
    @Test
    public void failureInPreparedStatments() throws InterruptedException {

        expectCloseAfterOperation(new ConnectionAction() {
            @Override
            public DbFuture invoke(Connection connection) {
                return connection.prepareQuery(FAIL_QUERY);
            }
        });
        expectCloseAfterOperation(new ConnectionAction() {
            @Override
            public DbFuture invoke(Connection connection) {
                return connection.prepareUpdate(FAIL_QUERY);
            }
        });
        expectCloseAfterOperation(new ConnectionAction() {
            @Override
            public DbFuture invoke(Connection connection) {
                try {
                    return connection.prepareQuery(AbstractMockPreparedStatement.FAIL_STATEMENT_EXECUTE)
                            .get().execute();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        expectCloseAfterOperation(new ConnectionAction() {
            @Override
            public DbFuture invoke(Connection connection) {
                try {
                    return connection.prepareQuery(AbstractMockPreparedStatement.FAIL_STATEMENT_EXECUTE)
                            .get().executeWithCallback(new DefaultResultEventsHandler(), new DefaultResultSet());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        expectCloseAfterOperation(new ConnectionAction() {
            @Override
            public DbFuture invoke(Connection connection) {
                try {
                    return connection.prepareUpdate(AbstractMockPreparedStatement.FAIL_STATEMENT_EXECUTE).get().execute();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
    @Test
    public void failureInTransactionConnection() throws InterruptedException {

        expectCloseAfterOperation(new ConnectionAction() {
            @Override
            public DbFuture invoke(Connection connection) {
                connection.beginTransaction();
                final MockConnection mockConnection = MockConnectionFactory.lastInstanceRequestedOnThisThread().lastInstanceRequestedOnThisThread();
                mockConnection.failTxOperation();
                return connection.rollback();
            }
        });
        expectCloseAfterOperation(new ConnectionAction() {
            @Override
            public DbFuture invoke(Connection connection) {
                connection.beginTransaction();
                final MockConnection mockConnection = MockConnectionFactory.lastInstanceRequestedOnThisThread().lastInstanceRequestedOnThisThread();
                mockConnection.failTxOperation();
                return connection.commit();
            }
        });
    }

    private void expectCloseAfterOperation(ConnectionAction toInvoke) throws InterruptedException {
        final ConnectionManager connectionManager
                = ConnectionManagerProvider.createConnectionManager("adbcj:pooled:mock:database", "sa", "pwd");

        final MockConnectionManager mockConnections = MockConnectionFactory.lastInstanceRequestedOnThisThread();

        final Connection connection = connectionManager.connect().get();
        try {
            toInvoke.invoke(connection).get();
            Assert.fail("Expect failure");
        } catch (Exception e) {
            // expected
        }

        connection.close();

        mockConnections.assertConnectionAlive(0);
    }
}
