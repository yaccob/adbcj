package org.adbcj.support;

import org.adbcj.ResultEventHandler;

/**
* @author roman.stoffel@gamlor.info
* @since 11.04.12
*/
public abstract class ExpectResultRequest<T> extends AbstractDbSession.Request<T> {
    private final ResultEventHandler<T> eventHandler;
    private final T accumulator;

    public ExpectResultRequest(AbstractDbSession session, ResultEventHandler<T> eventHandler, T accumulator) {
        super(session);
        this.eventHandler = eventHandler;
        this.accumulator = accumulator;
    }

    public ResultEventHandler<T> getEventHandler() {
        return eventHandler;
    }

    public T getAccumulator() {
        return accumulator;
    }
}
