package org.adbcj.connectionpool;

import org.adbcj.Connection;
import org.adbcj.ConnectionManager;
import org.adbcj.ConnectionManagerProvider;
import org.testng.annotations.Test;

/**
 * @author roman.stoffel@gamlor.info
 */
public class TransactionIsRolledBackTest {
    @Test
    public void closesPreparedStatements() throws InterruptedException {
        final ConnectionManager connectionManager
                = ConnectionManagerProvider.createConnectionManager("adbcj:pooled:mock:database", "sa", "pwd");

        final MockConnectionManager mockFactory = MockConnectionFactory.lastInstanceRequestedOnThisThread();

        final Connection connection = connectionManager.connect().get();

        connection.beginTransaction();

        final MockConnection mockConnection = mockFactory.lastInstanceRequestedOnThisThread();
        mockConnection.assertTransactionState(TransactionState.ACTIVE);

        connection.close();


        mockConnection.assertTransactionState(TransactionState.ROLLED_BACK);
    }
}
