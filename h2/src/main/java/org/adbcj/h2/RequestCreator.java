package org.adbcj.h2;

import org.adbcj.*;
import org.adbcj.h2.decoding.*;
import org.adbcj.h2.packets.*;
import org.adbcj.support.CancellationToken;
import org.adbcj.support.DefaultDbFuture;
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
        DefaultDbFuture<Void> future = new DefaultDbFuture<Void>(connection.stackTrachingOptions());
        return new Request("Close-Request", future, new CloseConnection(future, connection), new CloseCommand());
    }

    public <T> Request createQuery(String sql,
                                          ResultHandler<T> eventHandler,
                                          T accumulator) {
        CancellationToken cancelSupport = new CancellationToken();
        final int sessionId = connection.nextId();
        final int queryId = connection.nextId();
        DefaultDbFuture<T> resultFuture = new DefaultDbFuture<T>(connection.stackTrachingOptions(),cancelSupport);
        final Request executeQuery = executeQueryAndClose(sql,
                eventHandler,
                accumulator,
                resultFuture,
                cancelSupport,
                sessionId,
                queryId);
        return new Request("Prepare Query: " + sql,
                resultFuture,
                continueWithRequest(executeQuery, resultFuture),
                new QueryPrepareCommand(sessionId, sql,cancelSupport),
                executeQuery);
    }

    public Request executeUpdate(String sql) {
        CancellationToken cancelSupport = new CancellationToken();
        final int sessionId = connection.nextId();
        DefaultDbFuture<Result> resultFuture = new DefaultDbFuture<Result>(connection.stackTrachingOptions(),cancelSupport);
        final Request executeQuery = executeUpdateAndClose(sql, resultFuture,cancelSupport, sessionId);
        return new Request("Prepare Query: " + sql, resultFuture,
                continueWithRequest(executeQuery, resultFuture),
                new QueryPrepareCommand(sessionId, sql,cancelSupport),
                executeQuery);
    }

    public Request executePrepareQuery(String sql) {
        CancellationToken cancelSupport = new CancellationToken();
        DefaultDbFuture<PreparedQuery> resultFuture = new DefaultDbFuture<PreparedQuery>(connection.stackTrachingOptions(),cancelSupport);
        final int sessionId = connection.nextId();
        return new Request("Prepare Query: " + sql, resultFuture,
                createPrepareQuery(resultFuture, sessionId), new QueryPrepareCommand(sessionId, sql,cancelSupport));
    }


    public Request executePrepareUpdate(String sql) {
        CancellationToken cancelSupport = new CancellationToken();
        DefaultDbFuture<PreparedUpdate> resultFuture = new DefaultDbFuture<PreparedUpdate>(connection.stackTrachingOptions(),cancelSupport);
        final int sessionId = connection.nextId();
        return new Request("Prepare Update: " + sql, resultFuture,
                createPrepareUpdate(resultFuture, sessionId), new QueryPrepareCommand(sessionId, sql,cancelSupport));
    }

    public <T> Request executeQueryStatement(ResultHandler<T> eventHandler,
                                                    T accumulator,
                                                    int sessionId,
                                                    Object[] params) {
        CancellationToken cancelSupport = new CancellationToken();
        DefaultDbFuture<T> resultFuture = new DefaultDbFuture<T>(connection.stackTrachingOptions(),cancelSupport);
        int queryId = connection.nextId();
        return new Request("ExecutePreparedQuery", resultFuture,
                new QueryHeader<T>(SafeResultHandlerDecorator.wrap(eventHandler, resultFuture),
                        accumulator,
                        resultFuture,connection), new QueryExecute(sessionId, queryId,cancelSupport, params));
    }
    public Request executeUpdateStatement(int sessionId,
                                                 Object[] params) {
        CancellationToken cancelSupport = new CancellationToken();
        DefaultDbFuture<Result> resultFuture = new DefaultDbFuture<Result>(connection.stackTrachingOptions(),cancelSupport);
        return new Request("ExecutePreparedUpdate: ", resultFuture,
                new UpdateResult(resultFuture,connection),
                new CompoundCommand(cancelSupport,
                        new UpdateExecute(sessionId,cancelSupport,params),
                        new QueryExecute(connection.idForAutoId(), connection.nextId(),cancelSupport)));
    }
    public Request executeCloseStatement(int sessionId) {
        DefaultDbFuture<Void> resultFuture = new DefaultDbFuture<Void>(connection.stackTrachingOptions());
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
        return new Request("Begin Transacton",new DefaultDbFuture(connection.stackTrachingOptions()),
                new AwaitOk(connection),
                new AutoCommitChangeCommand(AutoCommitChangeCommand.AutoCommit.AUTO_COMMIT_OFF) );

    }
    public Request commitTransaction(){
        final DefaultDbFuture future = new DefaultDbFuture(connection.stackTrachingOptions());
        return new Request("Commit Transaction", future,
                new CompleteTransaction(future,connection),
                new CompoundCommand(CancellationToken.NO_CANCELLATION,
                        new UpdateExecute(connection.idForCommit(),CancellationToken.NO_CANCELLATION),
                        new AutoCommitChangeCommand(AutoCommitChangeCommand.AutoCommit.AUTO_COMMIT_ON) ));

    }
    public Request rollbackTransaction(){
        final DefaultDbFuture future = new DefaultDbFuture(connection.stackTrachingOptions());
        return new Request("Rollback Transaction", future,
                new CompleteTransaction(future,connection),
                new CompoundCommand(CancellationToken.NO_CANCELLATION,
                        new UpdateExecute(connection.idForRollback(),CancellationToken.NO_CANCELLATION),
                        new AutoCommitChangeCommand(AutoCommitChangeCommand.AutoCommit.AUTO_COMMIT_ON) ));

    }


    <T> StatementPrepare<T> continueWithRequest(final Request followUpRequest,
                                                              DefaultDbFuture<T> resultFuture){
        return new StatementPrepare<T>(resultFuture,connection) {
            @Override
            protected void handleCompletion(H2Connection connection, int paramsCount) {
                if(paramsCount==0){
                    connection.forceQueRequest(followUpRequest);
                }else{
                    throw new DbException("Implementation error: Expect 0 parameters, but got: "+paramsCount);
                }
            }
        };
    }


    StatementPrepare<PreparedUpdate> createPrepareUpdate(final DefaultDbFuture<PreparedUpdate> resultFuture,
                                                                final int sessionId) {
        return new StatementPrepare<PreparedUpdate>(resultFuture,connection) {
            @Override
            protected void handleCompletion(H2Connection connection, int paramsCount) {
                H2PreparedUpdate query = new H2PreparedUpdate(connection,sessionId,paramsCount);
                resultFuture.trySetResult(query);
            }
        };
    }


    StatementPrepare<PreparedQuery> createPrepareQuery(final DefaultDbFuture<PreparedQuery> resultFuture,
                                                              final int sessionId) {
        return new StatementPrepare<PreparedQuery>(resultFuture,connection) {
            @Override
            protected void handleCompletion(H2Connection connection, int paramsCount) {
                H2PreparedQuery query = new H2PreparedQuery(connection,sessionId,paramsCount);
                resultFuture.trySetResult(query);
            }
        };
    }


    <T> Request executeQueryAndClose(String sql, ResultHandler<T> eventHandler,
                                            T accumulator,
                                            DefaultDbFuture<T> resultFuture,
                                            CancellationToken cancelSupport,
                                            int sessionId,
                                            int queryId) {
        return new Request("ExecuteQuery: " + sql, resultFuture,
                new QueryHeader<T>(SafeResultHandlerDecorator.wrap(eventHandler, resultFuture),
                        accumulator,
                        resultFuture,connection),
                new CompoundCommand(cancelSupport,
                new QueryExecute(sessionId, queryId,cancelSupport),
                new CommandClose(sessionId)));
    }

    <T> Request executeUpdateAndClose(String sql,
                                      DefaultDbFuture<Result> resultFuture,
                                             CancellationToken cancelSupport,
                                             int sessionId) {
        return new Request("UpdateExecute: " + sql, resultFuture,
                new UpdateResult(resultFuture,connection),
                new CompoundCommand(cancelSupport,
                        new UpdateExecute(sessionId,cancelSupport),
                        new QueryExecute(connection.idForAutoId(), connection.nextId(),cancelSupport),
                        new CommandClose(sessionId)));
    }
}
