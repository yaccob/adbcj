package org.adbcj;

import org.adbcj.support.CancellationAction;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.stacktracing.StackTracingOptions;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class TestDefaultFuture {

    public static final CancellationAction CAN_CANCEL = new CancellationAction() {
        @Override
        public boolean cancel() {
            return true;
        }
    };

    @Test
    public void stateTransitionToComplete() throws InterruptedException {
        DefaultDbFuture<String> future = new DefaultDbFuture<String>(StackTracingOptions.GLOBAL_DEFAULT);
        Assert.assertEquals(FutureState.NOT_COMPLETED,future.getState());
        Assert.assertTrue(future.trySetResult("data"));
        Assert.assertEquals(FutureState.SUCCESS,future.getState());
        Assert.assertEquals("data",future.getResult());
        Assert.assertEquals(null,future.getException());
        Assert.assertEquals("data",future.get());

    }
    @Test
    public void stateTransitionToFailure() throws InterruptedException {
        DefaultDbFuture<String> future = new DefaultDbFuture<String>(StackTracingOptions.GLOBAL_DEFAULT);
        Assert.assertEquals(FutureState.NOT_COMPLETED, future.getState());
        Assert.assertTrue(future.trySetException(new Exception("Expected")));
        Assert.assertEquals(FutureState.FAILURE, future.getState());
        Assert.assertEquals("Expected",future.getException().getMessage());
        try{
            future.get();
            Assert.fail("Expected exception");
        } catch (DbException ex){
            // expected
        }
    }
    @Test
    public void stateTransitionToCancel()throws InterruptedException{
        DefaultDbFuture<String> future = new DefaultDbFuture<String>(StackTracingOptions.GLOBAL_DEFAULT,CAN_CANCEL);
        Assert.assertEquals(FutureState.NOT_COMPLETED,future.getState());
        Assert.assertTrue(future.cancel(true));
        Assert.assertEquals(FutureState.CANCELLED, future.getState());
        Assert.assertEquals(null,future.getException());
        try{
            future.get();
            Assert.fail("Expected cancellation");
        } catch (CancellationException ex){
            // expected
        }
    }
    @Test
    public void notCancelSupport()throws InterruptedException{
        DefaultDbFuture<String> notCancellable = new DefaultDbFuture<String>(StackTracingOptions.GLOBAL_DEFAULT,new CancellationAction() {
            @Override
            public boolean cancel() {
                return false;
            }
        });
        Assert.assertFalse(notCancellable.cancel(true));
        Assert.assertEquals(FutureState.NOT_COMPLETED, notCancellable.getState());


        DefaultDbFuture<String> noSupport = new DefaultDbFuture<String>(StackTracingOptions.GLOBAL_DEFAULT);
        Assert.assertFalse(noSupport.cancel(true));
        Assert.assertEquals(FutureState.NOT_COMPLETED, notCancellable.getState());
    }


    @Test
    public void fireListenerByCompleting()throws InterruptedException{
        checkListener(FutureState.SUCCESS, new ActionOnFuture() {
            @Override
            public void apply(DefaultDbFuture future) {
                future.trySetResult("result");
            }
        });

    }
    @Test
    public void fireListenerByFailing()throws InterruptedException{
        checkListener(FutureState.FAILURE, new ActionOnFuture() {
            @Override
            public void apply(DefaultDbFuture future) {
                future.trySetException(new Exception("Expected"));
            }
        });

    }
    @Test
    public void fireListenerByCancelling()throws InterruptedException{
        checkListener(FutureState.CANCELLED, new ActionOnFuture() {
            @Override
            public void apply(DefaultDbFuture future) {
                future.cancel(true);
            }
        });

    }
    @Test
    public void trySettingMyFail()throws InterruptedException{
        DefaultDbFuture<String> future = new DefaultDbFuture<String>(StackTracingOptions.GLOBAL_DEFAULT,CAN_CANCEL);
        future.setResult("completed");

        Assert.assertFalse(future.trySetResult("does not work"));
        Assert.assertFalse(future.trySetException(new Exception("does not work")));
        Assert.assertFalse(future.cancel(true));

        try{
            future.setResult("does not work");
            Assert.fail("Should throw");
        } catch (IllegalStateException ex){
            // expected
        }
        try{
            future.setException(new Exception("does not work"));
            Assert.fail("Should throw");
        } catch (IllegalStateException ex){
            // expected
        }

    }

    @Test
    public void wakesUp()throws InterruptedException{
        final DefaultDbFuture<String> toComplete = new DefaultDbFuture<String>(StackTracingOptions.GLOBAL_DEFAULT);
        final CountDownLatch nowCanRun = new CountDownLatch(1);
        new Thread(){
            @Override
            public void run() {
                try {
                    nowCanRun.await();
                    Thread.sleep(100);
                    toComplete.setResult("completed");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        try {
            toComplete.get(50,TimeUnit.MILLISECONDS);
            Assert.fail("Times out");
        } catch (TimeoutException e) {
            // expected
        }
        nowCanRun.countDown();
        Assert.assertEquals("completed",toComplete.get());

    }
    @Test
    public void canRemoveListener()throws InterruptedException{
        final CountDownLatch completed = new CountDownLatch(1);
        DefaultDbFuture<String> toComplete = new DefaultDbFuture<String>(StackTracingOptions.GLOBAL_DEFAULT);
        final DbListener<String> listener = new DbListener<String>() {
            @Override
            public void onCompletion(DbFuture<String> future) {
                completed.countDown();
            }
        };
        toComplete.addListener(listener);
        toComplete.removeListener(listener);
        toComplete.setResult("completed");

        final long count = completed.getCount();
        Assert.assertEquals(1,count);

    }
    @Test
    public void hasOriginalRequestStack()throws InterruptedException{
        DefaultDbFuture<String> toComplete = new DefaultDbFuture<String>(StackTracingOptions.FORCED_BY_INSTANCE);

        toComplete.setException(new DbException("Expected"));

        final DbException exception = toComplete.getException();
        Assert.assertEquals(exception.getMessage(), "Expected");
        checkStack(exception);

        try{
            toComplete.get();
        }catch (DbException ex){
            checkStack(ex);

        }

    }

    private void checkStack(DbException ex) {
        boolean foundThisTestInStack = false;
        for (StackTraceElement element : ex.getStackTrace()) {
            if(element.getMethodName().equals("hasOriginalRequestStack")){
                foundThisTestInStack = true;
            }
        }
        junit.framework.Assert.assertTrue(foundThisTestInStack);
    }

    private void checkListener(final FutureState expectedState, ActionOnFuture future) throws InterruptedException {
        checkListenerByCompletingFuture(expectedState, future);
        checkListenerOnCompletedFuture(expectedState, future);
    }

    private void checkListenerByCompletingFuture(final FutureState expectedState, ActionOnFuture future) throws InterruptedException {
        final CountDownLatch completed = new CountDownLatch(1);
        DefaultDbFuture<String> toComplete = new DefaultDbFuture<String>(StackTracingOptions.GLOBAL_DEFAULT,CAN_CANCEL);
        future.apply(toComplete);
        registerListener(expectedState, completed, toComplete);
        Assert.assertTrue(completed.await(10, TimeUnit.SECONDS));
    }

    private void checkListenerOnCompletedFuture(final FutureState expectedState, ActionOnFuture future) throws InterruptedException {
        final CountDownLatch completed = new CountDownLatch(1);
        DefaultDbFuture<String> toComplete = new DefaultDbFuture<String>(StackTracingOptions.GLOBAL_DEFAULT,CAN_CANCEL);
        registerListener(expectedState, completed, toComplete);
        future.apply(toComplete);
        Assert.assertTrue(completed.await(10, TimeUnit.SECONDS));
    }

    private void registerListener(final FutureState expectedState, final CountDownLatch completed, DefaultDbFuture<String> toComplete) {
        toComplete.addListener(new DbListener<String>() {
            @Override
            public void onCompletion(DbFuture<String> future) {
                Assert.assertEquals(expectedState, future.getState());
                completed.countDown();
            }
        });
    }


    static interface ActionOnFuture{
        void apply(DefaultDbFuture future);
    }
}
