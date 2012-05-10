package org.adbcj.tck.test;

import org.adbcj.ConnectionManager;
import org.adbcj.ConnectionManagerProvider;
import org.adbcj.DbFuture;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;

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
}
