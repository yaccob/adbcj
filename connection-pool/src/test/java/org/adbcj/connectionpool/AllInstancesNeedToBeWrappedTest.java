package org.adbcj.connectionpool;

import org.adbcj.Connection;
import org.adbcj.ConnectionManager;
import org.adbcj.ConnectionManagerProvider;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author roman.stoffel@gamlor.info
 */
public class AllInstancesNeedToBeWrappedTest {


    @Test
    public void allConnectionMethodsNeedToBeWrapped() throws InterruptedException {

        final ConnectionManager connectionManager = ConnectionManagerProvider.createConnectionManager("adbcj:pooled:mock:database", "sa", "pwd");

        final Connection firstConnection = connectionManager.connect().get();

        assertIsPooledResource(firstConnection.prepareQuery("valid").get());
        assertIsPooledResource(firstConnection.prepareUpdate("valid").get());
        assertIsPooledResource(firstConnection.getConnectionManager());
    }

    private void assertIsPooledResource(Object resource) {
        Assert.assertTrue(resource instanceof PooledResource,"Instance has to be a pooled resource: "+resource);
    }


}
