package org.adbcj.connectionpool;

import org.adbcj.ConnectionManager;
import org.adbcj.ConnectionManagerProvider;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author roman.stoffel@gamlor.info
 */
public class ConnectionPoolInstantiationTests {


    @Test
    public void canGetPooledConnection() throws InterruptedException {
        final ConnectionManager connectionManager = ConnectionManagerProvider.createConnectionManager("adbcj:pooled:mock:database", "sa", "pwd");

        Assert.assertNotNull(connectionManager);

    }

    @Test
    public void throwsOnIllegalUrl() throws InterruptedException {
        final String invalidUrl = "adbcj:pooled:wrong:pooled:mock:database";
        try {
            final ConnectionManager connectionManager = ConnectionManagerProvider.createConnectionManager(invalidUrl, "sa", "pwd");
            Assert.fail("Expect exception");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains(invalidUrl));
        }
    }
}
