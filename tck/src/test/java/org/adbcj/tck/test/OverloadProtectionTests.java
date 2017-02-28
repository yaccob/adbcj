package org.adbcj.tck.test;

import org.adbcj.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;


public class OverloadProtectionTests  extends AbstractWithConnectionManagerTest {

    @Test
    public void throwsOnOverloadWithRequests() throws Exception {
        final Connection connection = connectionManager.connect().get();
        List<Future> futures = new ArrayList<Future>();
        try{
            for(int i=0;i<256;i++){
                final Future<ResultSet> future = connection.executeQuery("SELECT 1");
                futures.add(future);

            }
            Assert.fail("Expected exception");
        } catch (DbException e){
            Assert.assertTrue(e.getMessage().contains("64"));
            Assert.assertTrue(e.getMessage().contains("requests"));
        }
        for (Future future : futures) {
            future.get();
        }

    }
}
