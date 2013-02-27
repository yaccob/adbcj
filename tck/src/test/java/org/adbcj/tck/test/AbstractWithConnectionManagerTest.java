package org.adbcj.tck.test;

import org.adbcj.CloseMode;
import org.adbcj.ConnectionManager;
import org.adbcj.ConnectionManagerProvider;
import org.adbcj.DbFuture;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;

import java.util.HashMap;
import java.util.Map;

/**
 * @author roman.stoffel@gamlor.info
 */
public abstract class AbstractWithConnectionManagerTest {
    protected ConnectionManager connectionManager;

    @Parameters({"url", "user", "password"})
    @BeforeClass
    public void createConnectionManager(String url, String user, String password) {
        connectionManager = ConnectionManagerProvider.createConnectionManager(url,
                user,
                password, properties());
    }

    protected Map<String,String> properties(){
        return new HashMap<>();
    }

    @AfterClass
    public void closeConnectionManager() throws InterruptedException {
        DbFuture<Void> closeFuture = connectionManager.close(CloseMode.CANCEL_PENDING_OPERATIONS);
        closeFuture.get();
    }
}
