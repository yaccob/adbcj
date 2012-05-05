package org.adbcj.tck.test;

import org.adbcj.*;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author roman.stoffel@gamlor.info
 * @since 05.04.12
 */
@Test(invocationCount=10, threadPoolSize=5, timeOut = 500000)
public class PreparedStatementsTest {
    private ConnectionManager connectionManager;

    @Parameters({"url", "user", "password"})
    @BeforeTest
    public void createConnectionManager(String url, String user, String password) {
        connectionManager = ConnectionManagerProvider.createConnectionManager(url, user, password);
    }
    @AfterTest
    public void closeConnectionManager() {
        DbFuture<Void> closeFuture = connectionManager.close(true);
        closeFuture.getUninterruptably();
    }
    public void testSimpleSelect() throws DbException, InterruptedException {
        Connection connection = connectionManager.connect().get();
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM simple_values" +
                " WHERE str_val LIKE ?").get();

        assertQueryFor(statement, "Zero");
        statement.close();
        connection.close();
    }
    public void testSelectWithNull() throws DbException, InterruptedException {
        Connection connection = connectionManager.connect().get();
        PreparedStatement statement = connection.prepareStatement("SELECT int_val,str_val,NULL FROM simple_values" +
                " WHERE str_val LIKE ?").get();

        ResultSet resultSet = statement.executeQuery("Zero").get();

        Assert.assertEquals(resultSet.size(), 1);
        Assert.assertEquals(resultSet.get(0).get(1).getString(), "Zero");
        Assert.assertEquals(resultSet.get(0).get(2).getString(), null);
        statement.close();
        connection.close();
    }

    public void testErrorIsReported() throws DbException, InterruptedException {
        Connection connection = connectionManager.connect().get();
        DbSessionFuture<PreparedStatement> future = connection.prepareStatement("SELECT * FROM this:is:an:invalid:query ");

        try{
            ResultSet rows = future.get().executeQuery().get();
            Assert.fail("Expected a failure, and not "+ rows);
        } catch (DbException ex){
            ex.printStackTrace();
            // expected
        }
        connection.close();
    }
    public void testCanReuseStatement() throws DbException, InterruptedException {
        Connection connection = connectionManager.connect().get();
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM simple_values" +
                " WHERE str_val LIKE ?").get();


        assertQueryFor(statement, "Zero");
        assertQueryFor(statement, "One");

        statement.close();
        connection.close();
    }
    public void testCanSelectNull() throws DbException, InterruptedException{
        Connection connection = connectionManager.connect().get();
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM `table_with_some_values` " +
                "WHERE `can_be_null_int` IS NULL").get();

        ResultSet resultSet = statement.executeQuery().get();
        Assert.assertEquals(resultSet.get(0).get(1).getString(), null);
        Assert.assertEquals(resultSet.get(0).get(2).getString(), null);


        statement.close();
        connection.close();
    }
    public void testCanCloseStatement() throws DbException, InterruptedException{
        Connection connection = connectionManager.connect().get();
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM `table_with_some_values` " +
                "WHERE `can_be_null_int` IS NULL").get();


        statement.close().get();
        Assert.assertTrue(statement.isClosed());
        connection.close();
    }

    private void assertQueryFor(PreparedStatement statement, String valueToQueryFor) throws InterruptedException {
        ResultSet resultSet = statement.executeQuery(valueToQueryFor).get();

        Assert.assertEquals(resultSet.size(), 1);
        Assert.assertEquals(resultSet.get(0).get(1).getString(), valueToQueryFor);
    }
}
