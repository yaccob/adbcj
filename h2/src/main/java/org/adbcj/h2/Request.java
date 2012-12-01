package org.adbcj.h2;

import org.adbcj.ResultHandler;
import org.adbcj.h2.decoding.CloseConnection;
import org.adbcj.h2.decoding.DecoderState;
import org.adbcj.h2.decoding.QueryHeader;
import org.adbcj.h2.decoding.QueryPrepare;
import org.adbcj.h2.packets.*;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.DefaultDbSessionFuture;

/**
 * @author roman.stoffel@gamlor.info
 */
public class Request {
    private final String description;
    private final DefaultDbFuture toComplete;
    private final DecoderState startState;
    private final ClientToServerPacket request;

    private Request(String description,
                    DefaultDbFuture toComplete,
                    DecoderState startState,
                    ClientToServerPacket request) {
        this.description = description;
        this.toComplete = toComplete;
        this.startState = startState;
        this.request = request;
    }

    public DefaultDbFuture<Object> getToComplete() {
        return toComplete;
    }

    public DecoderState getStartState() {
        return startState;
    }

    public static Request createCloseRequest(DefaultDbFuture<Void> future, H2Connection connection) {
        return new Request("Close-Request",future,new CloseConnection(future,connection),new CloseCommand());
    }

    public static <T> Request createQuery(String sql,
                                          ResultHandler<T> eventHandler,
                                          T accumulator,
                                          DefaultDbSessionFuture<T> resultFuture,
                                          int sessionId) {
        return new Request("Query Request: "+sql,resultFuture,
                new QueryPrepare<T>(SafeResultHandlerDecorator.wrap(eventHandler, resultFuture),
                        accumulator,
                        resultFuture,sessionId), new QueryPrepareCommand(sessionId, sql));
    }
    public static <T> Request executeQueryAndClose(ResultHandler<T> eventHandler,
                                                   T accumulator,
                                                   DefaultDbSessionFuture<T> resultFuture,
                                                   int sessionId,
                                                   int queryId) {
        return new Request("ExecuteQuery",resultFuture,
                new QueryHeader<T>(SafeResultHandlerDecorator.wrap(eventHandler, resultFuture),
                        accumulator,
                        resultFuture), new CompoundCommand(new QueryExecute(sessionId, queryId),new CommandClose(sessionId)));
    }

    @Override
    public String toString() {
        return String.valueOf(description);
    }

    public ClientToServerPacket getRequest() {
        return request;
    }
}
