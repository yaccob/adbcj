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
    public void canReadDifferentTexts() throws Exception{
        Connection connection = connectionManager.connect().get();
        ResultSet resultSet = connection.executeQuery("SELECT textData FROM textContent ORDER by lang").get();
        Assert.assertEquals(5,resultSet.size());

        Assert.assertEquals(resultSet.get(0).get("textData").getString(),"Die äüö sind toll");
        Assert.assertEquals(resultSet.get(1).get("textData").getString(),"English is a nice language");
        Assert.assertEquals(resultSet.get(2).get("textData").getString(),"ウィキペディア（英: Wikipedia）");
        Assert.assertEquals(resultSet.get(3).get("textData").getString(),"한국어 너무 좋다");
        Assert.assertEquals(resultSet.get(4).get("textData").getString(),"维基百科（英语：Wikipedia）");
    }
    @Test
    public void canReadWriteDelete() throws Exception{
        Connection connection = connectionManager.connect().get();
        connection.executeUpdate("INSERT INTO textContent (lang, textData) VALUES ('fa','ویکی‌پدیا (به انگلیسی: Wikipedia)')").get();
        ResultSet resultSet = connection.executeQuery("SELECT textData FROM textContent WHERE lang LIKE 'fa'").get();


        Assert.assertEquals(resultSet.get(0).get("textData").getString(),"ویکی‌پدیا (به انگلیسی: Wikipedia)");


        connection.executeUpdate("DELETE FROM textContent WHERE lang LIKE 'fa'").get();


        ResultSet checkDeleted = connection.executeQuery("SELECT textData FROM textContent WHERE lang LIKE 'fa'").get();
        Assert.assertEquals(checkDeleted.size(),0);
    }
}
