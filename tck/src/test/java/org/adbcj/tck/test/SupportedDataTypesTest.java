package org.adbcj.tck.test;

import org.adbcj.*;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.math.BigDecimal;

/**
 * @author roman.stoffel@gamlor.info
 * @since 03.05.12
 */
public class SupportedDataTypesTest {
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
    public void supportedInSelect() throws Exception {
        final Connection connection = connectionManager.connect().get();
        final ResultSet resultSet = connection.executeQuery("SELECT *, NULL FROM supporteddatatypes").get();
        final Row row = resultSet.get(0);

        assertValuesOfResult(row);

        connection.close().get();

    }

    @Test
    public void supportedInPreparedStatement() throws Exception {
        final Connection connection = connectionManager.connect().get();
        final PreparedStatement statement = connection.prepareStatement("SELECT *, NULL FROM supporteddatatypes").get();
        final ResultSet resultSet = statement.executeQuery().get();
        final Row row = resultSet.get(0);

        assertValuesOfResult(row);


        connection.close().get();

    }

    private void assertValuesOfResult(Row row) {
        Assert.assertEquals(row.get(0).getInt(), 42);
        Assert.assertEquals(row.get(1).getString(),"4242");
        Assert.assertEquals(row.get(2).getLong(),42L);
        Assert.assertEquals(row.get(3).getBigDecimal(),new BigDecimal("42"));
        Assert.assertTrue(row.get(4).getDate().getTime() < System.currentTimeMillis());
        Assert.assertEquals(row.get(5).getDouble(),42.42,0.0001);
        Assert.assertEquals(row.get(6).getString(),null);
    }
}
