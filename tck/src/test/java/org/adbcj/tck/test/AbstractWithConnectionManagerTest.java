package org.adbcj.tck.test;

import org.adbcj.ConnectionManager;
import org.adbcj.ConnectionManagerProvider;
import org.adbcj.DbFuture;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;

/**
 * @author roman.stoffel@gamlor.info
 */
public abstract class AbstractWithConnectionManagerTest {
    protected ConnectionManager connectionManager;

    @Parameters({"url", "user", "password"})
    @BeforeClass
    public void createConnectionManager(String url, String user, String password) {
        connectionManager = ConnectionManagerProvider.createConnectionManager(url, user, password);
    }

    @AfterClass
    public void closeConnectionManager() throws InterruptedException {
        DbFuture<Void> closeFuture = connectionManager.close();
        closeFuture.get();
    }
}
