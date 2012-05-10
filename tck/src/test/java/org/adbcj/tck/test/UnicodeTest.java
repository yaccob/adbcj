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
public class UnicodeTest {

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
    public void getDiffrentTexts() throws Exception{
        Connection connection = connectionManager.connect().get();
        ResultSet resultSet = connection.executeQuery("SELECT text FROM textContent ORDER by lang").get();
        Assert.assertEquals(5,resultSet.size());

        Assert.assertEquals(resultSet.get(0).get("text").getString(),"Die äüö sind toll");
        Assert.assertEquals(resultSet.get(1).get("text").getString(),"English is a nice language");
        Assert.assertEquals(resultSet.get(2).get("text").getString(),"ウィキペディア（英: Wikipedia）");
        Assert.assertEquals(resultSet.get(3).get("text").getString(),"한국어 너무 좋다");
        Assert.assertEquals(resultSet.get(4).get("text").getString(),"维基百科（英语：Wikipedia）");
    }
}
