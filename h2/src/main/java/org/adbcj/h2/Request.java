package org.adbcj.h2;

import org.adbcj.h2.decoding.CloseConnection;
import org.adbcj.h2.decoding.DecoderState;
import org.adbcj.support.DefaultDbFuture;

/**
 * @author roman.stoffel@gamlor.info
 */
public class Request {
    private final DefaultDbFuture toComplete;
    private final DecoderState startState;

    private Request(DefaultDbFuture toComplete, DecoderState startState) {
        this.toComplete = toComplete;
        this.startState = startState;
    }

    public DefaultDbFuture<Object> getToComplete() {
        return toComplete;
    }

    public DecoderState getStartState() {
        return startState;
    }

    public static Request createCloseRequest(DefaultDbFuture<Void> future) {
        return new Request(future,new CloseConnection(future));
    }
}
