package org.adbcj.tck.test;

import junit.framework.Assert;
import org.adbcj.Connection;
import org.adbcj.DbSessionFuture;
import org.adbcj.ResultSet;
import org.testng.annotations.Test;

import java.util.concurrent.CancellationException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class CancelingRequests extends AbstractWithConnectionManagerTest{


    @Test
    public void canCancelSelect() throws InterruptedException {
        final Connection connection = connectionManager.connect().get();

        final DbSessionFuture<ResultSet> result = connection.executeQuery("SELECT SLEEP(10)");
        Thread.sleep(1000);
        boolean cannotBeCanceled = result.cancel(true);

        Assert.assertFalse(cannotBeCanceled);

        connection.close();

    }
    @Test
    public void canChancelNotYetRunningStatement() throws InterruptedException {
        final Connection connection = connectionManager.connect().get();

        final DbSessionFuture<ResultSet> runningStatment = connection.executeQuery("SELECT SLEEP(5)");
        final DbSessionFuture<ResultSet> toCancel = connection.executeQuery("SELECT SLEEP(5)");
        boolean canCancel = toCancel.cancel(true);

        Assert.assertTrue(canCancel);

        try{
            toCancel.get();
            Assert.fail("Schould throw CancellationException");
        } catch (CancellationException expected){
            //expected
        }
        connection.close();

    }
}
