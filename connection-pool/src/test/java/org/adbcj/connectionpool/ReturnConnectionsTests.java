package org.adbcj.connectionpool;

import org.adbcj.Connection;
import org.adbcj.ConnectionManager;
import org.adbcj.ConnectionManagerProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author roman.stoffel@gamlor.info
 */
public class ReturnConnectionsTests {

    @Test
    public void reusesSingleConnection() throws InterruptedException {

        final ConnectionManager connectionManager = ConnectionManagerProvider.createConnectionManager("adbcj:pooled:mock:database", "sa", "pwd");
        final MockConnectionManager mockManager = MockConnectionFactory.lastInstanceRequestedOnThisThread();

        final Connection firstConnection = connectionManager.connect().get();
        mockManager.assertConnectionAlive(1);
        firstConnection.close().get();

        final Connection second = connectionManager.connect().get();
        mockManager.assertConnectionAlive(1);
        second.close().get();
        mockManager.assertConnectionAlive(1);

        mockManager.close().get();

        mockManager.assertWasClosed();
    }



    @Test
    public void cachesStatments() throws InterruptedException {

        final ConnectionManager connectionManager = ConnectionManagerProvider.createConnectionManager("adbcj:pooled:mock:database",
                "sa", "pwd");
        final MockConnectionManager mockManager = MockConnectionFactory.lastInstanceRequestedOnThisThread();

        final Connection firstConnection = connectionManager.connect().get();
        firstConnection.prepareQuery("Query 1").get();
        firstConnection.prepareUpdate("Update 1").get();
        firstConnection.close().get();

        final MockConnection conn = mockManager.lastInstanceRequestedOnThisThread();
        conn.assertAmountOfPreparedStatements(2);


        final Connection secondConnection = connectionManager.connect().get();
        secondConnection.prepareQuery("Query 1").get();
        secondConnection.prepareQuery("Update 1").get();
        secondConnection.prepareQuery("Other 2").get();
        secondConnection.close().get();

        conn.assertAmountOfPreparedStatements(3);
    }
    @Test
    public void closeSharedStatment() throws InterruptedException {

        final ConnectionManager connectionManager = ConnectionManagerProvider.createConnectionManager("adbcj:pooled:mock:database",
                "sa", "pwd",twoItemCache());
        final MockConnectionManager mockManager = MockConnectionFactory.lastInstanceRequestedOnThisThread();
        final Connection firstConnection = connectionManager.connect().get();
        final MockConnection conn = mockManager.lastInstanceRequestedOnThisThread();

        firstConnection.prepareQuery("Query 1").get().close();
        conn.assertAmountOfPreparedStatements(1);
        firstConnection.close().get();
        conn.assertAmountOfPreparedStatements(1);

        final Connection secondConnection = connectionManager.connect().get();
        secondConnection.prepareQuery("Query 1").get();
        secondConnection.prepareUpdate("Update 1").get();
        secondConnection.prepareUpdate("Update 2").get();
        secondConnection.prepareUpdate("Update 3").get();
        conn.assertAmountOfPreparedStatements(4);
        secondConnection.close().get();

        conn.assertAmountOfPreparedStatements(2);
    }


    private Map<String, String> twoItemCache() {
        final Map<String,String> noStmtCache = new HashMap<String, String>();
        noStmtCache.put(ConfigInfo.STATEMENT_CACHE_SIZE,"2");
        return noStmtCache;
    }
}
