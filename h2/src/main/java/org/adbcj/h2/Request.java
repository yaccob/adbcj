package org.adbcj.h2;

import org.adbcj.PreparedQuery;
import org.adbcj.Result;
import org.adbcj.ResultHandler;
import org.adbcj.h2.decoding.*;
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

    @Override
    public String toString() {
        return String.valueOf(description);
    }

    public ClientToServerPacket getRequest() {
        return request;
    }

    public static <T> Request createQuery(String sql,
                                          ResultHandler<T> eventHandler,
                                          T accumulator,
                                          DefaultDbSessionFuture<T> resultFuture,
                                          int sessionId) {
        int queryId = ((H2Connection)resultFuture.getSession()).nextId();
        final Request executeQuery = executeQueryAndClose(sql,eventHandler, accumulator, resultFuture, sessionId, queryId);
        return new Request("Prepare Query: "+sql,resultFuture,
                QueryPrepare.continueWithRequest(executeQuery,resultFuture, sessionId), new QueryPrepareCommand(sessionId, sql));
    }

    public static Request executeUpdate(String sql, DefaultDbSessionFuture<Result> resultFuture, int sessionId) {
        final Request executeQuery = executeUpdateAndClose(sql, resultFuture, sessionId);
        return new Request("Prepare Query: "+sql,resultFuture,
                QueryPrepare.continueWithRequest(executeQuery,resultFuture, sessionId), new QueryPrepareCommand(sessionId, sql));
    }
    public static Request executePrepareQuery(String sql, DefaultDbSessionFuture<PreparedQuery> resultFuture, int sessionId) {
        return new Request("Prepare Query: "+sql,resultFuture,
                QueryPrepare.createPrepareQuery(resultFuture, sessionId), new QueryPrepareCommand(sessionId, sql));
    }

    public static <T> Request executeQueryStatement(ResultHandler<T> eventHandler,
                                            T accumulator,
                                            DefaultDbSessionFuture<T> resultFuture,
                                            int sessionId,
                                            int queryId,
                                            Object[] params) {
        return new Request("ExecutePreparedQuery: ",resultFuture,
                new QueryHeader<T>(SafeResultHandlerDecorator.wrap(eventHandler, resultFuture),
                        accumulator,
                        resultFuture), new QueryExecute(sessionId, queryId,params));
    }

    static <T> Request executeQueryAndClose(String sql,ResultHandler<T> eventHandler,
                                            T accumulator,
                                            DefaultDbSessionFuture<T> resultFuture,
                                            int sessionId,
                                            int queryId) {
        return new Request("ExecuteQuery: "+sql,resultFuture,
                new QueryHeader<T>(SafeResultHandlerDecorator.wrap(eventHandler, resultFuture),
                        accumulator,
                        resultFuture), new CompoundCommand(new QueryExecute(sessionId, queryId, new Object[0]),new CommandClose(sessionId)));
    }

    static <T> Request executeUpdateAndClose(String sql,
                                            DefaultDbSessionFuture<Result> resultFuture,
                                            int sessionId) {
        return new Request("ExecuteUpdate: "+sql,resultFuture,
                new UpdateResult(resultFuture), new CompoundCommand(new ExecuteUpdate(sessionId),new CommandClose(sessionId)));
    }

    public static Request executeCloseStatement(H2Connection connection,DefaultDbFuture<Void> resultFuture, int sessionId) {
        return new Request("ExecuteCloseStatement: ",resultFuture,
                new AnswerNextRequest(connection), new CommandClose(sessionId,resultFuture));
    }
}
