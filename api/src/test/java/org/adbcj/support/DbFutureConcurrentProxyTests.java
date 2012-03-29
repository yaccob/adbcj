package org.adbcj.support;

import junit.framework.Assert;
import org.adbcj.DbException;
import org.adbcj.DbFuture;
import org.adbcj.DbListener;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author roman.stoffel@gamlor.info
 * @since 29.03.12
 */
public class DbFutureConcurrentProxyTests {
    private final ExecutorService threadPool = Executors.newCachedThreadPool();

    @Test
    public void finishingWithExceptionCallsListeners() throws InterruptedException {
        final CountDownLatch expectEvent = new CountDownLatch(1);
        final DbFutureConcurrentProxy<String> future = new DbFutureConcurrentProxy<String>();
        future.addListener(new DbListener<String>() {
            @Override
            public void onCompletion(DbFuture<String> stringDbFuture) throws Exception {
                expectEvent.countDown();
            }
        });

        threadPool.submit(new Runnable() {
            @Override
            public void run() {
                future.setException(new DbException("FAIL"));
                future.setDone();
            }
        });

        Assert.assertTrue(expectEvent.await(5, TimeUnit.SECONDS));
        try{
            future.get();
            Assert.fail("Expected exception");
        }catch (DbException e){
            // expected
        }

    }

}
