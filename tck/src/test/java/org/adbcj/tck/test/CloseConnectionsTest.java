package org.adbcj.tck.test;

import junit.framework.Assert;
import org.adbcj.*;
import org.adbcj.support.AbstractDbSession;
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



    @Test
    public void canCancelSelect() throws InterruptedException {
        final Connection connection = connectionManager.connect().get();

        final DbSessionFuture<ResultSet> rs1 = connection.executeQuery("SELECT int_val, str_val " +
                "FROM simple_values where str_val LIKE 'Not-In-Database-Value'");
        final DbSessionFuture<ResultSet> rs2 = connection.executeQuery("SELECT int_val, str_val " +
                "FROM simple_values where str_val LIKE 'Not-In-Database-Value'");
        final DbSessionFuture<ResultSet> rs3 = connection.executeQuery("SELECT int_val, str_val " +
                "FROM simple_values where str_val LIKE 'Not-In-Database-Value'");


        ((AbstractDbSession)connection).errorPendingRequests(new Exception("Expect This Exception"));

        try{
            rs1.get();
            rs2.get();
            rs3.get();
        }catch (Exception e){
            Assert.assertTrue(e.getMessage().contains("Expect This Exception"));
        }

        connection.close();
    }
}
