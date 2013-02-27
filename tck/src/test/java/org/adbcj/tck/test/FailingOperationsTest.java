package org.adbcj.tck.test;

import junit.framework.Assert;
import org.adbcj.Connection;
import org.adbcj.DbException;
import org.adbcj.StandardProperties;
import org.testng.annotations.Test;

import java.util.Map;

/**
 * @author roman.stoffel@gamlor.info
 */
public class FailingOperationsTest extends AbstractWithConnectionManagerTest {

    @Test
    public void failingQuery() throws InterruptedException {
        Connection connection = connectionManager.connect().get();
        try{
            connection.executeQuery("SELECT invalid query so it will throw").get();
            Assert.fail("Expect failure");
        }catch (DbException ex){
            Assert.assertTrue(ex.getSuppressed().length>=1);

        }

    }

    @Override
    protected Map<String, String> properties() {
        final Map<String, String> config = super.properties();
        config.put(StandardProperties.CAPTURE_CALL_STACK,"true");
        return config;
    }
}
