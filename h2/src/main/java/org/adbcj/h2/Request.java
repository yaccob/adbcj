package org.adbcj.h2;

import org.adbcj.*;
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
    /**
     * A blocking request is one where the operation is split across multiple
     * request-responses, but are treated as one logical operation.
     * To avoid invalid interleaving, we need to ensure that now other request is sent
     * during the communication for such a request.
     */
    private final Request blocksFor;

    private Request(String description,
                    DefaultDbFuture toComplete,
                    DecoderState startState,
                    ClientToServerPacket request) {
        this.description = description;
        this.toComplete = toComplete;
        this.startState = startState;
        this.request = request;
        this.blocksFor = null;
    }
    private Request(String description,
                    DefaultDbFuture toComplete,
                    DecoderState startState,
                    ClientToServerPacket request, Request blocksFor) {
        this.description = description;
        this.toComplete = toComplete;
        this.startState = startState;
        this.request = request;
        this.blocksFor = blocksFor;
    }

    public DefaultDbFuture<Object> getToComplete() {
        return toComplete;
    }

    public DecoderState getStartState() {
        return startState;
    }

    public static Request createCloseRequest(DefaultDbFuture<Void> future, H2Connection connection) {
        return new Request("Close-Request", future, new CloseConnection(future, connection), new CloseCommand());
    }

    public boolean isBlocking() {
        return blocksFor!=null;
    }

    public boolean unblockBy(Request nextRequest) {
        return blocksFor==nextRequest;
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
        int queryId = ((H2Connection) resultFuture.getSession()).nextId();
        final Request executeQuery = executeQueryAndClose(sql, eventHandler, accumulator, resultFuture, sessionId, queryId);
        return new Request("Prepare Query: " + sql,
                resultFuture,
                StatementPrepare.continueWithRequest(executeQuery, resultFuture),
                new QueryPrepareCommand(sessionId, sql),
                executeQuery);
    }

    public static Request executeUpdate(String sql, DefaultDbSessionFuture<Result> resultFuture) {
        int sessionId = nextId(resultFuture);
        final Request executeQuery = executeUpdateAndClose(sql, resultFuture, sessionId);
        return new Request("Prepare Query: " + sql, resultFuture,
                StatementPrepare.continueWithRequest(executeQuery, resultFuture),
                new QueryPrepareCommand(sessionId, sql),
                executeQuery);
    }

    public static Request executePrepareQuery(String sql, DefaultDbSessionFuture<PreparedQuery> resultFuture) {
        int sessionId = nextId(resultFuture);
        return new Request("Prepare Query: " + sql, resultFuture,
                StatementPrepare.createPrepareQuery(resultFuture, sessionId), new QueryPrepareCommand(sessionId, sql));
    }


    public static Request executePrepareUpdate(String sql, DefaultDbSessionFuture<PreparedUpdate> resultFuture) {
        int sessionId = nextId(resultFuture);
        return new Request("Prepare Update: " + sql, resultFuture,
                StatementPrepare.createPrepareUpdate(resultFuture, sessionId), new QueryPrepareCommand(sessionId, sql));
    }

    public static <T> Request executeQueryStatement(ResultHandler<T> eventHandler,
                                                    T accumulator,
                                                    DefaultDbSessionFuture<T> resultFuture,
                                                    int sessionId,
                                                    int queryId,
                                                    Object[] params) {
        return new Request("ExecutePreparedQuery: ", resultFuture,
                new QueryHeader<T>(SafeResultHandlerDecorator.wrap(eventHandler, resultFuture),
                        accumulator,
                        resultFuture), new QueryExecute(sessionId, queryId, params));
    }
    public static Request executeUpdateStatement(DefaultDbSessionFuture<Result> resultFuture,
                                                 int sessionId,
                                                 Object[] params) {
        H2Connection connection = ((H2Connection) resultFuture.getSession());
        return new Request("ExecutePreparedUpdate: ", resultFuture,
                new UpdateResult(resultFuture),
                new CompoundCommand(
                        new UpdateExecute(sessionId,params),
                        new QueryExecute(connection.idForAutoId(), nextId(resultFuture))));
    }
    public static Request executeCloseStatement(H2Connection connection, DefaultDbFuture<Void> resultFuture, int sessionId) {
        return new Request("ExecuteCloseStatement: ", resultFuture,
                new AnswerNextRequest(connection), new CommandClose(sessionId, resultFuture));
    }

    public static Request createGetAutoIdStatement(int sessionId, DefaultDbFuture<Connection> completeConnection,
                                                   H2Connection connection) {
        String sql = "SELECT SCOPE_IDENTITY() WHERE SCOPE_IDENTITY() IS NOT NULL";
        return new Request("Prepare Query: " + sql, completeConnection,
                StatementPrepare.createAutoIdCompletion(completeConnection, connection), new QueryPrepareCommand(sessionId, sql));
    }

    public static Request beginTransaction(H2Connection connection){
        return new Request("Begin Transacton",new DefaultDbFuture(),
                new AwaitOk(connection),
                new BeginTransactionCommand() );

    }

    static <T> Request executeQueryAndClose(String sql, ResultHandler<T> eventHandler,
                                            T accumulator,
                                            DefaultDbSessionFuture<T> resultFuture,
                                            int sessionId,
                                            int queryId) {
        return new Request("ExecuteQuery: " + sql, resultFuture,
                new QueryHeader<T>(SafeResultHandlerDecorator.wrap(eventHandler, resultFuture),
                        accumulator,
                        resultFuture), new CompoundCommand(new QueryExecute(sessionId, queryId), new CommandClose(sessionId)));
    }

    static <T> Request executeUpdateAndClose(String sql,
                                             DefaultDbSessionFuture<Result> resultFuture,
                                             int sessionId) {
        H2Connection connection = (H2Connection) resultFuture.getSession();
        return new Request("UpdateExecute: " + sql, resultFuture,
                new UpdateResult(resultFuture),
                new CompoundCommand(
                        new UpdateExecute(sessionId),
                        new QueryExecute(connection.idForAutoId(), connection.nextId()),
                        new CommandClose(sessionId)));
    }

    private static int nextId(DefaultDbSessionFuture<?> resultFuture) {
        return ((H2Connection)resultFuture.getSession()).nextId();
    }

}
