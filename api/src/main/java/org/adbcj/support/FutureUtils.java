package org.adbcj.support;

import org.adbcj.DbFuture;
import org.adbcj.DbListener;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class FutureUtils {
    private FutureUtils(){}




    protected static <TArgument,TResult> DbListener<TArgument> createTransformationListener(
            final OneArgFunction<TArgument, TResult> transformation,
            final DefaultDbFuture<TResult> completion) {
        return new DbListener<TArgument>() {
            @Override
            public void onCompletion(DbFuture<TArgument> future) {
                switch (future.getState()){
                    case SUCCESS:
                        completion.trySetResult(transformation.apply(future.getResult()));
                        break;
                    case FAILURE:
                        completion.trySetException(future.getException());
                        break;
                    case CANCELLED:
                        completion.trySetCancelled();
                        break;
                }
            }
        };
    }

    public static <TArgument,TResult> DefaultDbFuture<TResult> map(final DbFuture<TArgument> originalFuture,
                                                             final OneArgFunction<TArgument, TResult> tranformation) {
        DefaultDbFuture<TResult> completion = new DefaultDbFuture<TResult>(new CancellationAction() {
            @Override
            public boolean cancel() {
                return originalFuture.cancel(true);
            }
        });
        originalFuture.addListener(createTransformationListener(tranformation,completion));
        return completion;
    }
}
