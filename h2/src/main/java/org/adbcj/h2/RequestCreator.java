package org.adbcj.h2;

import org.adbcj.*;
import org.adbcj.h2.decoding.*;
import org.adbcj.h2.packets.*;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.DefaultDbSessionFuture;
import org.adbcj.support.SafeResultHandlerDecorator;

/**
 * @author roman.stoffel@gamlor.info
 */
public class RequestCreator {
    private final H2Connection connection;

    public RequestCreator(H2Connection connection) {
        this.connection = connection;
    }


    public Request createCloseRequest() {
        DefaultDbSessionFuture<Void> future = new DefaultDbSessionFuture<Void>(connection);
        return new Request("Close-Request", future, new CloseConnection(future, connection), new CloseCommand());
    }

    public <T> Request createQuery(String sql,
                                          ResultHandler<T> eventHandler,
                                          T accumulator) {
        CancellationToken cancelSupport = new CancellationToken();
        final int sessionId = connection.nextId();
        final int queryId = connection.nextId();
        DefaultDbSessionFuture<T> resultFuture = new DefaultDbSessionFuture<T>(connection,cancelSupport);
        final Request executeQuery = executeQueryAndClose(sql,
                eventHandler,
                accumulator,
                resultFuture,
                cancelSupport,
                sessionId,
                queryId);
        return new Request("Prepare Query: " + sql,
                resultFuture,
                StatementPrepare.continueWithRequest(executeQuery, resultFuture),
                new QueryPrepareCommand(sessionId, sql,cancelSupport),
                executeQuery);
    }

    public Request executeUpdate(String sql) {
        CancellationToken cancelSupport = new CancellationToken();
        final int sessionId = connection.nextId();
        DefaultDbSessionFuture<Result> resultFuture = new DefaultDbSessionFuture<Result>(connection,cancelSupport);
        final Request executeQuery = executeUpdateAndClose(sql, resultFuture,cancelSupport, sessionId);
        return new Request("Prepare Query: " + sql, resultFuture,
                StatementPrepare.continueWithRequest(executeQuery, resultFuture),
                new QueryPrepareCommand(sessionId, sql,cancelSupport),
                executeQuery);
    }

    public Request executePrepareQuery(String sql) {
        CancellationToken cancelSupport = new CancellationToken();
        DefaultDbSessionFuture<PreparedQuery> resultFuture = new DefaultDbSessionFuture<PreparedQuery>(connection,cancelSupport);
        final int sessionId = connection.nextId();
        return new Request("Prepare Query: " + sql, resultFuture,
                StatementPrepare.createPrepareQuery(resultFuture, sessionId), new QueryPrepareCommand(sessionId, sql,cancelSupport));
    }


    public Request executePrepareUpdate(String sql) {
        CancellationToken cancelSupport = new CancellationToken();
        DefaultDbSessionFuture<PreparedUpdate> resultFuture = new DefaultDbSessionFuture<PreparedUpdate>(connection,cancelSupport);
        final int sessionId = connection.nextId();
        return new Request("Prepare Update: " + sql, resultFuture,
                StatementPrepare.createPrepareUpdate(resultFuture, sessionId), new QueryPrepareCommand(sessionId, sql,cancelSupport));
    }

    public <T> Request executeQueryStatement(ResultHandler<T> eventHandler,
                                                    T accumulator,
                                                    int sessionId,
                                                    Object[] params) {
        CancellationToken cancelSupport = new CancellationToken();
        DefaultDbSessionFuture<T> resultFuture = new DefaultDbSessionFuture<T>(connection,cancelSupport);
        int queryId = connection.nextId();
        return new Request("ExecutePreparedQuery", resultFuture,
                new QueryHeader<T>(SafeResultHandlerDecorator.wrap(eventHandler, resultFuture),
                        accumulator,
                        resultFuture), new QueryExecute(sessionId, queryId,cancelSupport, params));
    }
    public Request executeUpdateStatement(int sessionId,
                                                 Object[] params) {
        CancellationToken cancelSupport = new CancellationToken();
        DefaultDbSessionFuture<Result> resultFuture = new DefaultDbSessionFuture<Result>(connection,cancelSupport);
        return new Request("ExecutePreparedUpdate: ", resultFuture,
                new UpdateResult(resultFuture),
                new CompoundCommand(cancelSupport,
                        new UpdateExecute(sessionId,cancelSupport,params),
                        new QueryExecute(connection.idForAutoId(), connection.nextId(),cancelSupport)));
    }
    public Request executeCloseStatement() {
        DefaultDbSessionFuture<Void> resultFuture = new DefaultDbSessionFuture<Void>(connection);
        final int sessionId = connection.nextId();
        return new Request("ExecuteCloseStatement: ", resultFuture,
                new AnswerNextRequest(connection), new CommandClose(sessionId, resultFuture));
    }

    public Request createGetAutoIdStatement(DefaultDbFuture<Connection> completeConnection) {
        final int sessionId = connection.idForAutoId();
        String sql = "SELECT SCOPE_IDENTITY() WHERE SCOPE_IDENTITY() IS NOT NULL";
        return new Request("Prepare Query: " + sql, completeConnection,
                StatementPrepare.createOnlyPassFailure(completeConnection, connection),
                new QueryPrepareCommand(sessionId, sql,CancellationToken.NO_CANCELLATION));
    }
    public Request createCommitStatement(DefaultDbFuture<Connection> completeConnection) {
        final int sessionId = connection.idForCommit();
        String sql = "COMMIT";
        return new Request("Prepare Query: " + sql, completeConnection,
                StatementPrepare.createOnlyPassFailure(completeConnection, connection),
                new QueryPrepareCommand(sessionId, sql,CancellationToken.NO_CANCELLATION));
    }
    public Request createRollbackStatement(DefaultDbFuture<Connection> completeConnection) {
        final int sessionId = connection.idForRollback();
        String sql = "ROLLBACK";
        return new Request("Prepare Query: " + sql, completeConnection,
                StatementPrepare.completeFuture(completeConnection, connection),
                new QueryPrepareCommand(sessionId, sql,CancellationToken.NO_CANCELLATION));
    }

    public Request beginTransaction(){
        return new Request("Begin Transacton",new DefaultDbSessionFuture(connection),
                new AwaitOk(connection),
                new AutoCommitChangeCommand(AutoCommitChangeCommand.AutoCommit.AUTO_COMMIT_OFF) );

    }
    public Request commitTransaction(){
        final DefaultDbSessionFuture future = new DefaultDbSessionFuture(connection);
        return new Request("Commit Transaction", future,
                new CompleteTransaction(future),
                new CompoundCommand(CancellationToken.NO_CANCELLATION,
                        new UpdateExecute(connection.idForCommit(),CancellationToken.NO_CANCELLATION),
                        new AutoCommitChangeCommand(AutoCommitChangeCommand.AutoCommit.AUTO_COMMIT_ON) ));

    }
    public Request rollbackTransaction(){
        final DefaultDbSessionFuture future = new DefaultDbSessionFuture(connection);
        return new Request("Rollback Transaction", future,
                new CompleteTransaction(future),
                new CompoundCommand(CancellationToken.NO_CANCELLATION,
                        new UpdateExecute(connection.idForRollback(),CancellationToken.NO_CANCELLATION),
                        new AutoCommitChangeCommand(AutoCommitChangeCommand.AutoCommit.AUTO_COMMIT_ON) ));

    }

    <T> Request executeQueryAndClose(String sql, ResultHandler<T> eventHandler,
                                            T accumulator,
                                            DefaultDbSessionFuture<T> resultFuture,
                                            CancellationToken cancelSupport,
                                            int sessionId,
                                            int queryId) {
        return new Request("ExecuteQuery: " + sql, resultFuture,
                new QueryHeader<T>(SafeResultHandlerDecorator.wrap(eventHandler, resultFuture),
                        accumulator,
                        resultFuture),
                new CompoundCommand(cancelSupport,
                new QueryExecute(sessionId, queryId,cancelSupport),
                new CommandClose(sessionId)));
    }

    <T> Request executeUpdateAndClose(String sql,
                                             DefaultDbSessionFuture<Result> resultFuture,
                                             CancellationToken cancelSupport,
                                             int sessionId) {
        H2Connection connection = (H2Connection) resultFuture.getSession();
        return new Request("UpdateExecute: " + sql, resultFuture,
                new UpdateResult(resultFuture),
                new CompoundCommand(cancelSupport,
                        new UpdateExecute(sessionId,cancelSupport),
                        new QueryExecute(connection.idForAutoId(), connection.nextId(),cancelSupport),
                        new CommandClose(sessionId)));
    }
}
