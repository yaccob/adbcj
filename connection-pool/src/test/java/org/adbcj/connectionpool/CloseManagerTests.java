package org.adbcj.connectionpool;

import org.adbcj.Connection;
import org.adbcj.ConnectionManager;
import org.adbcj.ConnectionManagerProvider;
import org.testng.annotations.Test;

/**
 * @author roman.stoffel@gamlor.info
 */
public class CloseManagerTests {

    @Test
    public void reusesSingleConnection() throws InterruptedException {

        final ConnectionManager connectionManager = ConnectionManagerProvider.createConnectionManager("adbcj:pooled:mock:database", "sa", "pwd");
        final MockConnectionManager mockManager = MockConnectionFactory.lastInstanceRequestedOnThisThread();

        final Connection firstConnection = connectionManager.connect().get();
        mockManager.assertConnectionAlive(1);
        firstConnection.close().get();

        connectionManager.close().get();


        mockManager.assertWasClosed();
    }
}
