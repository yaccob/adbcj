package org.adbcj.tck.test;

import org.adbcj.*;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.Date;

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
    @Test
    public void canBindDatatypesToParameters() throws Exception {
        final Connection connection = connectionManager.connect().get();
        final PreparedStatement statement = connection.prepareStatement("SELECT *, NULL FROM supporteddatatypes " +
                "WHERE intColumn=? " +
                "AND varCharColumn LIKE ? " +
                "AND bigIntColumn = ? " +
                "AND decimalColumn = ? " +
                "AND dateColumn < ? " +
                "AND doubleColumn < ? ").get();
        final ResultSet resultSet = statement.executeQuery(42,
                "4242",
                42L,
                new BigDecimal("42"),
                new Date(),
                42.4200001).get();
        final Row row = resultSet.get(0);
        Assert.assertNotNull(row);


        connection.close().get();

    }
    @Test
    public void canBindNullToParameter() {
        Assert.fail();
    }

    private void assertValuesOfResult(Row row) {
        Assert.assertEquals(row.get("intColumn").getInt(), 42);
        Assert.assertEquals(row.get("varCharColumn").getString(),"4242");
        Assert.assertEquals(row.get("bigIntColumn").getLong(),42L);
        Assert.assertEquals(row.get("decimalColumn").getBigDecimal(),new BigDecimal("42"));
        Assert.assertTrue(row.get("dateColumn").getDate().getTime() < System.currentTimeMillis());
        Assert.assertEquals(row.get("doubleColumn").getDouble(),42.42,0.0001);
        Assert.assertEquals(row.get(6).getString(),null);
    }
}
