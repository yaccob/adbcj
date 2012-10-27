package org.adbcj.tck.test;

import org.adbcj.Connection;
import org.adbcj.PreparedUpdate;
import org.adbcj.Result;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author roman.stoffel@gamlor.info
 * @since 10.05.12
 */
public class InsertTest extends AbstractWithConnectionManagerTest{
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
        Assert.assertTrue(result.getGeneratedKeys().get(1).get(0).getLong() > 0);
        Assert.assertTrue(result.getGeneratedKeys().get(2).get(0).getLong()>0);

        connection.close();
    }
    @Test
    public void returnsAutoIncrementPreparedQuery() throws Exception{
        Connection connection = connectionManager.connect().get();
        PreparedUpdate statement = connection.prepareUpdate("INSERT INTO tableWithAutoId (textData) " +
                "VALUES (?)").get();
        Result result = statement.execute("value prepared").get();

        Assert.assertEquals(result.getAffectedRows(), 1L);
        Assert.assertTrue(result.getGeneratedKeys().get(0).get(0).getLong()>0);

        connection.close();
    }
}
