package org.adbcj.tck.test;

import org.adbcj.*;
import org.adbcj.tck.NoArgAction;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author roman.stoffel@gamlor.info
 */
public class CloseConnectionsTest {

    private ConnectionManager connectionManager;

    @Parameters({"url", "user", "password"})
    @BeforeTest
    public void createConnectionManager(String url, String user, String password) {
        connectionManager = ConnectionManagerProvider.createConnectionManager(url, user, password);
    }

    @AfterTest
    public void closeConnectionManager() throws InterruptedException {
        DbFuture<Void> closeFuture = connectionManager.close();
        closeFuture.get();
    }

    @Parameters({"url", "user", "password"})
    @Test
    public void closingManagerClosesConnections(String url, String user, String password) throws InterruptedException {
        final ConnectionManager manager = ConnectionManagerProvider.createConnectionManager(url, user, password);
        final Connection c1 = manager.connect().get();
        final DbFuture<ResultSet> runningQuery = c1.executeQuery("SELECT SLEEP(2)");
        final DbFuture<ResultSet> runningQuery2 = c1.executeQuery("SELECT SLEEP(2)");
        final Connection c2 = manager.connect().get();
        c2.beginTransaction();
        manager.close().get();


        checkClosed(c1, runningQuery, runningQuery2, c2);
    }

    @Parameters({"url", "user", "password"})
    @Test
    public void foreClosingManagerClosesConnections(String url, String user, String password) throws InterruptedException {
        final ConnectionManager manager = ConnectionManagerProvider.createConnectionManager(url, user, password);
        final Connection c1 = manager.connect().get();
        final Connection c2 = manager.connect().get();
        c2.beginTransaction();
        final DbFuture<ResultSet> runningQuery = c1.executeQuery("SELECT SLEEP(3)");
        final DbFuture<ResultSet> runningQuery2 = c1.executeQuery("SELECT SLEEP(4)");
        manager.close(CloseMode.CANCEL_PENDING_OPERATIONS).get();


        checkClosed(c1, runningQuery, runningQuery2, c2);


        Assert.assertTrue(runningQuery2.getState()==FutureState.SUCCESS
                || runningQuery2.getState()==FutureState.FAILURE,"Why is state: "+runningQuery2.getState());
    }


    @Test
    public void closingConnectionDoesNotAcceptNewRequests() throws InterruptedException {
        final Connection connection = connectionManager.connect().get();
        final PreparedQuery preparedSelect = connection.prepareQuery("SELECT 1").get();
        final PreparedUpdate preparedUpdate = connection.prepareUpdate("SELECT 1").get();
        final DbFuture<ResultSet> runningQuery = connection.executeQuery("SELECT SLEEP(5)");
        final DbFuture<Void> closeFuture = connection.close();
        Assert.assertTrue(connection.isClosed());

        shouldThrowException(new NoArgAction() {
            @Override
            public void invoke() {
                connection.executeQuery("SELECT 1");
            }
        });
        shouldThrowException(new NoArgAction() {
            @Override
            public void invoke() {
                connection.beginTransaction();
            }
        });
        shouldThrowException(new NoArgAction() {
            @Override
            public void invoke() {
                connection.prepareQuery("SELECT 1");
            }
        });
        shouldThrowException(new NoArgAction() {
            @Override
            public void invoke() {
                preparedSelect.execute();
            }
        });
        shouldThrowException(new NoArgAction() {
            @Override
            public void invoke() {
                preparedUpdate.execute();
            }
        });
        closeFuture.get();

    }

    @Test
    public void forceCloseConnections() throws InterruptedException {
        final Connection connection = connectionManager.connect().get();

        final DbFuture<ResultSet> rs1 = connection.executeQuery("SELECT int_val, str_val " +
                "FROM simple_values where str_val LIKE 'Not-In-Database-Value'");
        final DbFuture<ResultSet> rs2 = connection.executeQuery("SELECT int_val, str_val " +
                "FROM simple_values where str_val LIKE 'Not-In-Database-Value'");
        final DbFuture<ResultSet> rs3 = connection.executeQuery("SELECT int_val, str_val " +
                "FROM simple_values where str_val LIKE 'Not-In-Database-Value'");

        connection.close(CloseMode.CANCEL_PENDING_OPERATIONS);

        try {
            rs1.get();
            rs2.get();
            rs3.get();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

    private void checkClosed(final Connection c1, DbFuture<ResultSet> runningQuery, DbFuture<ResultSet> runningQuery2, Connection c2) {
        shouldThrowException(new NoArgAction() {
            @Override
            public void invoke() {
                c1.executeQuery("SELECT 1");
            }
        });

        Assert.assertTrue(c1.isClosed());
        Assert.assertTrue(c2.isClosed());
        Assert.assertTrue(runningQuery.isDone());
        Assert.assertTrue(runningQuery2.isDone());
    }

    private void shouldThrowException(NoArgAction toInvoke) {
        try {
            toInvoke.invoke();
            Assert.fail("Expect exception telling us that the connection is closing");
        } catch (DbSessionClosedException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

}
