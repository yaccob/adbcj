package org.adbcj.tck.test;

import org.adbcj.*;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author roman.stoffel@gamlor.info
 * @since 10.05.12
 */
public class InsertTest {

    private ConnectionManager connectionManager;

    @Parameters({"url", "user", "password"})
    @BeforeTest
    public void createConnectionManager(String url, String user, String password) {
        connectionManager = ConnectionManagerProvider.createConnectionManager(url, user, password);
    }
    @AfterTest
    public void closeConnectionManager() {
        DbFuture<Void> closeFuture = connectionManager.close();
        closeFuture.getUninterruptably();
    }

    @Test
    public void returnsAutoIncrement() throws Exception{
        Connection connection = connectionManager.connect().get();
        Result result = connection.executeUpdate("INSERT INTO tableWithAutoId (textData) VALUES ('data')").get();
        Assert.assertEquals(result.getAffectedRows(), 1L);
        Assert.assertTrue(result.getGeneratedKeys().get(0).get(0).getLong()>0);

        connection.close();
    }
    @Test
    public void returnsMutlitpleAutoIncrement() throws Exception{
        Connection connection = connectionManager.connect().get();
        Result result = connection.executeUpdate("INSERT INTO tableWithAutoId (textData) " +
                "VALUES ('data1'),('data2'),('data3');").get();
        Assert.assertEquals(result.getAffectedRows(), 3L);
        Assert.assertTrue(result.getGeneratedKeys().get(0).get(0).getLong()>0);
        Assert.assertTrue(result.getGeneratedKeys().get(1).get(0).getLong()>0);
        Assert.assertTrue(result.getGeneratedKeys().get(2).get(0).getLong()>0);

        connection.close();
    }
}
