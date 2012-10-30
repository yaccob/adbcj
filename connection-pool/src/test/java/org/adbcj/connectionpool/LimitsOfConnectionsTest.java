package org.adbcj.connectionpool;

import junit.framework.Assert;
import org.adbcj.Connection;
import org.adbcj.ConnectionManager;
import org.adbcj.ConnectionManagerProvider;
import org.adbcj.DbFuture;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class LimitsOfConnectionsTest {

    @Test
    public void maxConnectionsOne() throws Exception{
        Map<String, String> config = new HashMap<String, String>();
        config.put("pool.maxConnections","1");
        final ConnectionManager connectionManager
                = ConnectionManagerProvider.createConnectionManager("adbcj:pooled:mock:database",
                "sa", "pwd", config);
        final MockConnectionManager mockManager = MockConnectionFactory.lastInstanceRequestedOnThisThread();

        final Connection openConnection = connectionManager.connect().get();
        final DbFuture<Connection> connectFuture = connectionManager.connect();
        try{
            connectFuture.get(100, TimeUnit.MILLISECONDS);
            Assert.fail("");
        }catch (TimeoutException e){
            // expected
        }

        mockManager.assertMaxConnectonsUsed(1);

        openConnection.close().get();

        connectFuture.get();

        connectionManager.close().get();
    }


    @Test
    public void defaultIs50() throws Exception{
        final ConnectionManager connectionManager
                = ConnectionManagerProvider.createConnectionManager("adbcj:pooled:mock:database",
                "sa", "pwd");
        final MockConnectionManager mockManager = MockConnectionFactory.lastInstanceRequestedOnThisThread();

        ArrayList<Connection> directlyOpened = new ArrayList<Connection>();
        for(int i=0;i<50;i++){
            directlyOpened.add(connectionManager.connect().get());
        }
        ArrayList<DbFuture<Connection>> waitingOpened = new ArrayList<DbFuture<Connection>>();
        for(int i=0;i<20;i++){
            waitingOpened.add(connectionManager.connect());
        }
        for (Connection conn : directlyOpened) {
            conn.close().get();
        }
        for (DbFuture<Connection> waitingForConn : waitingOpened) {
            waitingForConn.get();
        }
        for (DbFuture<Connection> waitingForConn : waitingOpened) {
            waitingForConn.get().close();
        }

        mockManager.assertMaxConnectonsUsed(50);

        connectionManager.close().get();
    }
}
