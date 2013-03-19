package org.adbcj.tck.test;

import junit.framework.Assert;
import org.adbcj.*;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author roman.stoffel@gamlor.info
 */
public class OverloadProtectionTests  extends AbstractWithConnectionManagerTest {

    @Test
    public void throwsOnOverloadWithRequests() throws Exception {
        final Connection connection = connectionManager.connect().get();
        List<DbFuture> futures = new ArrayList<DbFuture>();
        try{
            for(int i=0;i<256;i++){
                final DbFuture<ResultSet> future = connection.executeQuery("SELECT 1");
                futures.add(future);

            }
            Assert.fail("Expected exception");
        } catch (DbException e){
            Assert.assertTrue(e.getMessage().contains("64"));
            Assert.assertTrue(e.getMessage().contains("requests"));
        }
        for (DbFuture future : futures) {
            future.get();
        }

    }
}
