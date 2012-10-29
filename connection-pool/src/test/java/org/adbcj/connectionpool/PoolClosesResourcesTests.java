package org.adbcj.connectionpool;

import org.adbcj.Connection;
import org.adbcj.ConnectionManager;
import org.adbcj.ConnectionManagerProvider;
import org.testng.annotations.Test;

/**
 * @author roman.stoffel@gamlor.info
 */
public class PoolClosesResourcesTests {

    @Test
    public void closesPreparedStatements() throws InterruptedException {
        final ConnectionManager connectionManager
                = ConnectionManagerProvider.createConnectionManager("adbcj:pooled:mock:database", "sa", "pwd");

        final MockConnectionManager mockFactory = MockConnectionFactory.lastInstanceRequestedOnThisThread();

        final Connection connection = connectionManager.connect().get();

        final MockConnection mockConnection = mockFactory.lastInstanceRequestedOnThisThread();

        connection.prepareUpdate("UPDATE something").get();
        connection.prepareUpdate("UPDATE something").get();
        mockConnection.assertAmountOfPreparedStatements(2);

        connection.close().get();

        mockConnection.assertAmountOfPreparedStatements(0);
    }

    @Test
    public void canCloseStatmentRegularly() throws InterruptedException {
        final ConnectionManager connectionManager
                = ConnectionManagerProvider.createConnectionManager("adbcj:pooled:mock:database", "sa", "pwd");

        final MockConnectionManager mockFactory = MockConnectionFactory.lastInstanceRequestedOnThisThread();

        final Connection connection = connectionManager.connect().get();

        final MockConnection mockConnection = mockFactory.lastInstanceRequestedOnThisThread();

        connection.prepareUpdate("UPDATE something").get().close().get();
        connection.prepareUpdate("UPDATE something").get();
        mockConnection.assertAmountOfPreparedStatements(1);

        connection.close().get();

        mockConnection.assertAmountOfPreparedStatements(0);
    }


}
