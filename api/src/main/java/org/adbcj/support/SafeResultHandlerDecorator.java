package org.adbcj.support;

import org.adbcj.Field;
import org.adbcj.ResultHandler;
import org.adbcj.Value;

/**
 * In case of an exception this handler will
 * pass that exception to the underlying result handlers {@link ResultHandler#exception(Throwable, Object)}
 *
 * @author roman.stoffel@gamlor.info
 */
public class SafeResultHandlerDecorator<T> implements ResultHandler<T> {
    private final ResultHandler<T> original;
    private final DefaultDbFuture<T> future;


    private SafeResultHandlerDecorator(ResultHandler<T> original, DefaultDbFuture<T> future) {
        this.original = original;
        this.future = future;
    }

    public static <T> ResultHandler<T> wrap(ResultHandler<T> eventHandler, DefaultDbFuture<T> resultFuture) {
        if(eventHandler instanceof SafeResultHandlerDecorator){
            return eventHandler;
        } else{
            return new SafeResultHandlerDecorator<T>(eventHandler, resultFuture);
        }
    }

    public void startFields(T accumulator) {
        try {
            original.startFields(accumulator);
        } catch (Throwable e) {
            exception(e, accumulator);
        }
    }

    public void field(Field field, T accumulator) {
        try {
            original.field(field, accumulator);
        } catch (Throwable e) {
            exception(e, accumulator);
        }
    }

    public void endFields(T accumulator) {
        try {
            original.endFields(accumulator);
        } catch (Throwable e) {
            exception(e, accumulator);
        }
    }

    public void startResults(T accumulator) {
        try {
            original.startResults(accumulator);
        } catch (Throwable e) {
            exception(e, accumulator);
        }
    }

    public void startRow(T accumulator) {
        try {
            original.startRow(accumulator);
        } catch (Throwable e) {
            exception(e, accumulator);
        }
    }

    public void value(Value value, T accumulator) {
        try {
            original.value(value, accumulator);
        } catch (Throwable e) {
            exception(e, accumulator);
        }
    }

    public void endRow(T accumulator) {
        try {
            original.endRow(accumulator);
        } catch (Throwable e) {
            exception(e, accumulator);
        }
    }

    public void endResults(T accumulator) {
        try {
            original.endResults(accumulator);
        } catch (Throwable e) {
            exception(e, accumulator);
        }
    }

    public void exception(Throwable t, T accumulator) {
        future.trySetException(t);
        original.exception(t, accumulator);
    }
}
