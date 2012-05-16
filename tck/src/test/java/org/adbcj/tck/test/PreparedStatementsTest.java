package org.adbcj.tck.test;

import org.adbcj.*;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author roman.stoffel@gamlor.info
 * @since 05.04.12
 */
@Test(invocationCount=3, threadPoolSize=5, timeOut = 500000)
public class PreparedStatementsTest {
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

    public void testSimpleSelect() throws DbException, InterruptedException {
        Connection connection = connectionManager.connect().get();
        PreparedQuery statement = connection.prepareQuery("SELECT * FROM simple_values" +
                " WHERE str_val LIKE ?").get();

        assertQueryFor(statement, "Zero");
        statement.close();
        connection.close();
    }
    public void testOrderIsCorrect() throws DbException, InterruptedException {
        Connection connection = connectionManager.connect().get();
        PreparedQuery statement = connection.prepareQuery("SELECT * FROM simple_values" +
                " WHERE int_val > 0 ORDER BY int_val DESC").get();

        ResultSet resultSet = statement.execute().get();
        Assert.assertEquals(resultSet.size(),4);
        Assert.assertEquals(resultSet.get(0).get(0).getInt(),4);
        Assert.assertEquals(resultSet.get(1).get(0).getInt(),3);
        Assert.assertEquals(resultSet.get(2).get(0).getInt(),2);
        Assert.assertEquals(resultSet.get(3).get(0).getInt(),1);


        statement.close();
        connection.close();
    }
    public void testSelectWithNull() throws DbException, InterruptedException {
        Connection connection = connectionManager.connect().get();
        PreparedQuery statement = connection.prepareQuery("SELECT int_val,str_val,NULL FROM simple_values" +
                " WHERE str_val LIKE ?").get();

        ResultSet resultSet = statement.execute("Zero").get();

        Assert.assertEquals(resultSet.size(), 1);
        Assert.assertEquals(resultSet.get(0).get(1).getString(), "Zero");
        Assert.assertEquals(resultSet.get(0).get(2).getString(), null);
        statement.close();
        connection.close();
    }

    public void testErrorIsReported() throws DbException, InterruptedException {
        Connection connection = connectionManager.connect().get();
        DbSessionFuture<PreparedQuery> future = connection.prepareQuery("SELECT * FROM this:is:an:invalid:query ");

        try{
            ResultSet rows = future.get().execute().get();
            Assert.fail("Expected a failure, and not "+ rows);
        } catch (DbException ex){
            // expected
        }
        connection.close();
    }
    public void testCanReuseStatement() throws DbException, InterruptedException {
        Connection connection = connectionManager.connect().get();
        PreparedQuery statement = connection.prepareQuery("SELECT * FROM simple_values" +
                " WHERE str_val LIKE ?").get();


        assertQueryFor(statement, "Zero");
        assertQueryFor(statement, "One");

        statement.close();
        connection.close();
    }
    public void testCanSelectNull() throws DbException, InterruptedException{
        Connection connection = connectionManager.connect().get();
        PreparedQuery statement = connection.prepareQuery("SELECT * FROM table_with_some_values " +
                "WHERE `can_be_null_int` IS NULL").get();

        ResultSet resultSet = statement.execute().get();
        Assert.assertEquals(resultSet.get(0).get(1).getString(), null);
        Assert.assertEquals(resultSet.get(0).get(2).getString(), null);


        statement.close();
        connection.close();
    }
    public void testCanCloseStatement() throws DbException, InterruptedException{
        Connection connection = connectionManager.connect().get();
        PreparedQuery statement = connection.prepareQuery("SELECT * FROM `table_with_some_values` " +
                "WHERE `can_be_null_int` IS NULL").get();


        statement.close().get();
        Assert.assertTrue(statement.isClosed());
        connection.close();
    }
    public void _testThrowsOnInvalidArgumentCount() throws DbException, InterruptedException{
        Connection connection = connectionManager.connect().get();
        PreparedQuery statement = connection.prepareQuery("SELECT * FROM simple_values" +
                " WHERE str_val LIKE ?").get();
        try{
            statement.execute("1","2","3").get();
            Assert.fail("Expect exception");
        } catch (DbException e){
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        } catch (IllegalArgumentException e){
            // expected
        }
        connection.close();
    }

    public void _testWorksWithCallback() throws Exception{

        Connection connection = connectionManager.connect().get();

        PreparedQuery query = connection.prepareQuery("SELECT str_val FROM simple_values " +
                " WHERE str_val LIKE ?").get();


        DbSessionFuture<StringBuilder> resultFuture = query.executeWithCallback(SelectTest.buildStringInCallback(),
                new StringBuilder(),"Zero");

        StringBuilder result = resultFuture.get();
        Assert.assertEquals(result.toString(), SelectTest.expectedStringFromCallback());

        connection.close();

    }

    public void testExceptionInCallbackHandler() throws Exception {
        Connection connection = connectionManager.connect().get();

        final CountDownLatch latch = new CountDownLatch(1);

        PreparedQuery query = connection.prepareQuery("SELECT str_val FROM simple_values " +
                " WHERE str_val LIKE ?").get();
        DbSessionFuture<StringBuilder> resultFuture = query.executeWithCallback(new AbstractEventHandler<StringBuilder>() {
            @Override
            public void startFields(StringBuilder accumulator) {
                throw new RuntimeException("Failure here");
            }

            @Override
            public void exception(Throwable t, StringBuilder accumulator) {
                latch.countDown();
            }
        }, new StringBuilder(),"Zero");
        Assert.assertTrue(latch.await(5, TimeUnit.SECONDS), "Expect that exception method is called");
        try {
            resultFuture.get();
            Assert.fail("Expected exception to be propagated");
        } catch (DbException e) {
            Assert.assertTrue(e.getMessage().contains("Failure here"));

        }
    }


    private void assertQueryFor(PreparedQuery statement, String valueToQueryFor) throws InterruptedException {
        ResultSet resultSet = statement.execute(valueToQueryFor).get();

        Assert.assertEquals(resultSet.size(), 1);
        Assert.assertEquals(resultSet.get(0).get(1).getString(), valueToQueryFor);
    }
}
