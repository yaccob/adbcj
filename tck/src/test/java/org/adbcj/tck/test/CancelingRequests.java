package org.adbcj.tck.test;

import org.adbcj.Connection;
import org.adbcj.DbFuture;
import org.adbcj.ResultSet;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.CancellationException;


public class CancelingRequests extends AbstractWithConnectionManagerTest{


    @Test
    public void canCancelSelect() throws InterruptedException {
        final Connection connection = connectionManager.connect().get();

        final DbFuture<ResultSet> result = connection.executeQuery("SELECT SLEEP(2)");
        Thread.sleep(500);
        boolean cannotBeCanceled = result.cancel(true);

        Assert.assertFalse(cannotBeCanceled);

        connection.close();

    }
    @Test
    public void mayCanChancelNotYetRunningStatement() throws InterruptedException {
        final Connection connection = connectionManager.connect().get();


        final DbFuture<ResultSet> runningStatment = connection.executeQuery("SELECT SLEEP(1)");
        final DbFuture<ResultSet> toCancel = connection.executeQuery("SELECT SLEEP(2)");
        boolean canCancel = toCancel.cancel(true);

        if(canCancel){
            try{
                toCancel.get();
                Assert.fail("Should throw CancellationException");
            } catch (CancellationException expected){
                //expected
            }
        }

        connection.close();

    }
}
