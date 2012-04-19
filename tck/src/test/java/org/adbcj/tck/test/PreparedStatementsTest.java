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
@Test(invocationCount=1, threadPoolSize=1, timeOut = 500000)
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
    }

//    public void testErrorIsReported() throws DbException, InterruptedException {
//        Connection connection = connectionManager.connect().get();
//        DbSessionFuture<PreparedStatement> future = connection.prepareStatement("SELECT * FROM this:is:an:invalid:query ");
//
//        try{
//            ResultSet rows = future.get().executeQuery().get();
//            Assert.fail("Expected a failure, and not "+ rows);
//        } catch (DbException ex){
//            ex.printStackTrace();
//            // expected
//        }
//    }
//    public void testCanReuseStatement() throws DbException, InterruptedException {
//        Connection connection = connectionManager.connect().get();
//        PreparedStatement statement = connection.prepareStatement("SELECT * FROM simple_values" +
//                " WHERE str_val LIKE ?").get();
//
//
//        assertQueryFor(statement, "Zero");
//        assertQueryFor(statement, "One");
//    }

    private void assertQueryFor(PreparedStatement statement, String valueToQueryFor) throws InterruptedException {
        ResultSet resultSet = statement.executeQuery(valueToQueryFor).get();

        Assert.assertEquals(resultSet.size(), 1);
        Assert.assertEquals(resultSet.get(0).get(1).getString(), valueToQueryFor);
    }
}
