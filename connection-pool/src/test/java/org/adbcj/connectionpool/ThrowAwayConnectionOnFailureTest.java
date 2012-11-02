package org.adbcj.connectionpool;

import junit.framework.Assert;
import org.adbcj.*;
import org.adbcj.support.DefaultResultEventsHandler;
import org.adbcj.support.DefaultResultSet;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.adbcj.connectionpool.MockConnection.FAIL_QUERY;

/**
 * @author roman.stoffel@gamlor.info
 */
public class ThrowAwayConnectionOnFailureTest {

    @Test
    public void canGetNewConnectionsForDamagedOnes() throws InterruptedException {
        Map<String,String> settings = new HashMap<String,String>();
        settings.put(ConfigInfo.POOL_MAX_CONNECTIONS,"1");
        final ConnectionManager connectionManager
                = ConnectionManagerProvider.createConnectionManager("adbcj:pooled:mock:database",
                "sa", "pwd",settings);
        final MockConnectionManager mockConnections = MockConnectionFactory.lastInstanceRequestedOnThisThread();

        expectCloseAfterOperation(connectionManager,new ConnectionAction() {
            @Override
            public DbFuture invoke(Connection connection) {
                return connection.executeQuery("fail-query",new DefaultResultEventsHandler(), new DefaultResultSet());
            }
        });

        final Connection connection = connectionManager.connect().get();
        mockConnections.assertConnectionAlive(1);
        connection.close(CloseMode.CANCEL_PENDING_OPERATIONS);

    }

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

        expectCloseAfterOperation(connectionManager,toInvoke);
    }

    private void expectCloseAfterOperation(ConnectionManager connectionManager,ConnectionAction toInvoke) throws InterruptedException {
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
