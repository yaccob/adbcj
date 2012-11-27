package org.adbcj.h2;

import org.adbcj.ResultHandler;
import org.adbcj.h2.decoding.CloseConnection;
import org.adbcj.h2.decoding.DecoderState;
import org.adbcj.h2.decoding.QueryPrepare;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.DefaultDbSessionFuture;

/**
 * @author roman.stoffel@gamlor.info
 */
public class Request {
    private final String description;
    private final DefaultDbFuture toComplete;
    private final DecoderState startState;

    private Request(String description,DefaultDbFuture toComplete, DecoderState startState) {
        this.description = description;
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
        return new Request("Close-Request",future,new CloseConnection(future));
    }

    public static <T> Request createQuery(String sql, ResultHandler<T> eventHandler, T accumulator, DefaultDbSessionFuture<T> resultFuture) {
        return new Request("Query Request: "+sql,resultFuture,new QueryPrepare(sql,eventHandler,accumulator,resultFuture));
    }

    @Override
    public String toString() {
        return String.valueOf(description);
    }
}
