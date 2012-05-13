package org.adbcj;

/**
 * A empty implementation of the {@link ResultEventHandler} interface to avoid
 * implementing each and every method.
 * @author roman.stoffel@gamlor.info
 * @since 13.05.12
 */
public abstract class AbstractEventHandler<T> implements ResultEventHandler<T> {
    @Override
    public void startFields(T accumulator) {
    }

    @Override
    public void field(Field field, T accumulator) {
    }

    @Override
    public void endFields(T accumulator) {
    }

    @Override
    public void startResults(T accumulator) {
    }

    @Override
    public void startRow(T accumulator) {
    }

    @Override
    public void value(Value value, T accumulator) {
    }

    @Override
    public void endRow(T accumulator) {
    }

    @Override
    public void endResults(T accumulator) {
    }

    @Override
    public void exception(Throwable t, T accumulator) {
    }
}
