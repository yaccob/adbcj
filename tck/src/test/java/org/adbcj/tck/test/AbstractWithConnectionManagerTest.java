package org.adbcj.tck.test;

import org.adbcj.CloseMode;
import org.adbcj.ConnectionManager;
import org.adbcj.ConnectionManagerProvider;
import org.adbcj.DbFuture;
import org.adbcj.tck.InitDatabase;
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
    private InitDatabase init;

    @Parameters({"jdbcUrl", "url", "user", "password", "setupClass"})
    @BeforeClass
    public void createConnectionManager(String jdbcUrl,
                                        String url,
                                        String user,
                                        String password,
                                        String setupClass) throws Exception {
        InitDatabase init = (InitDatabase) Class.forName(setupClass).newInstance();
        init.prepareMySQL(jdbcUrl, user, password);
        this.init = init;
        connectionManager = ConnectionManagerProvider.createConnectionManager(url,
                user,
                password, properties());
    }

    protected Map<String, String> properties() {
        return new HashMap<>();
    }

    @Parameters({"jdbcUrl", "user", "password",})
    @AfterClass
    public void closeConnectionManager(String jdbcUrl,
                                       String user,
                                       String password) throws InterruptedException {
        DbFuture<Void> closeFuture = connectionManager.close(CloseMode.CANCEL_PENDING_OPERATIONS);
        closeFuture.get();
        init.cleanUp(jdbcUrl, user, password);
    }
}
