package org.adbcj.tck.test;

import junit.framework.Assert;
import org.adbcj.Connection;
import org.adbcj.DbException;
import org.testng.annotations.Test;

/**
 * @author roman.stoffel@gamlor.info
 */
public class OverloadProtectionTests  extends AbstractWithConnectionManagerTest {

    @Test
    public void throwsOnOverloadWithRequests() throws Exception {
        final Connection connection = connectionManager.connect().get();

        try{
            for(int i=0;i<256;i++){
                connection.executeQuery("SELECT 1");

            }
            Assert.fail("Expected exception");
        } catch (DbException e){
            Assert.assertTrue(e.getMessage().contains("64"));
            Assert.assertTrue(e.getMessage().contains("requests"));
        }


    }
}
