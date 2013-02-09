package org.adbcj.tck.test;

import junit.framework.Assert;
import org.adbcj.Connection;
import org.adbcj.DbFuture;
import org.adbcj.DbListener;
import org.testng.annotations.Test;

/**
 * @author roman.stoffel@gamlor.info
 */
public class VoidIsVoidTest extends AbstractWithConnectionManagerTest{

    @Test
    public void testCommitTransaction() throws Exception {
        Connection connection = connectionManager.connect().get();
        try {
            connection.beginTransaction();
            assertFutureIsVoid(connection.commit());
        } finally {
            connection.close();
        }
    }
    @Test
    public void testRollbackTransaction() throws Exception {
        Connection connection = connectionManager.connect().get();
        try {
            connection.beginTransaction();
            assertFutureIsVoid(connection.rollback());
        } finally {
            connection.close();
        }
    }
    @Test
    public void testClose() throws Exception {
        Connection connection = connectionManager.connect().get();
        try {
            final DbFuture<Void> future = connection.close();
            assertFutureIsVoid(future);
        } finally {
            connection.close();
        }
    }

    private void assertFutureIsVoid(DbFuture<Void> future) throws InterruptedException {
        future.addListener(new DbListener<Void>() {
            @Override
            public void onCompletion(DbFuture<Void> future) {
                final Object object = future.getResult();
                Assert.assertTrue(object == null || object instanceof Void);
            }
        });
        final Object object = future.get();
        Assert.assertTrue(object == null || object instanceof Void);
    }
}
