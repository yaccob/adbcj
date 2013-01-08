package org.adbcj.mysql.codec;

import org.adbcj.PreparedQuery;
import org.adbcj.Result;
import org.adbcj.ResultHandler;
import org.adbcj.mysql.codec.decoding.*;
import org.adbcj.mysql.codec.packets.*;
import org.adbcj.support.CancellationToken;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.DefaultDbSessionFuture;
import org.adbcj.support.SafeResultHandlerDecorator;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class MySqlRequests {
    public static MySqlRequest createCloseRequest(MySqlConnection connection) {
        DefaultDbFuture<Void> future = new DefaultDbFuture<Void>();

        return new MySqlRequest("Close",future, new ExpectOK<Void>(future, connection),new CommandRequest(Command.QUIT));
    }
    public static <T> MySqlRequest executeQuery(String query, ResultHandler<T> eventHandler, T accumulator, MySqlConnection connection) {
        CancellationToken cancelSupport = new CancellationToken();
        DefaultDbSessionFuture<T> future = new DefaultDbSessionFuture<T>(connection,cancelSupport);
        ResultHandler<T> handleFailures = SafeResultHandlerDecorator.wrap(eventHandler, future);
        return new MySqlRequest("Query: "+query,future,
                new ExpectQueryResult<T>(Row.RowDecodingType.STRING_BASED, future, handleFailures,accumulator),
                new StringCommandRequest(Command.QUERY,query,cancelSupport));
    }
    public static <T> MySqlRequest executePreparedQuery(StatementPreparedEOF stmp,
                                                        Object[] data,
                                                        ResultHandler<T> eventHandler,
                                                        T accumulator, MySqlConnection connection) {
        DefaultDbSessionFuture<T> future = new DefaultDbSessionFuture<T>(connection);
        ResultHandler<T> handleFailures = SafeResultHandlerDecorator.wrap(eventHandler, future);
        return new MySqlRequest("Execute-Statement",future,
                new ExpectStatementResult(Row.RowDecodingType.BINARY, future, handleFailures,accumulator),
                new PreparedStatementRequest(stmp.getHandlerId(),stmp.getParametersTypes(),data));
    }

    public static MySqlRequest executeUpdate(String sql, MySqlConnection connection) {
        CancellationToken cancelSupport = new CancellationToken();
        DefaultDbSessionFuture<Result> future = new DefaultDbSessionFuture<Result>(connection,cancelSupport);
        return new MySqlRequest("Update: "+sql,future,
                new ExpectUpdateResult(future),
                new StringCommandRequest(Command.QUERY,sql,cancelSupport));
    }

    public static MySqlRequest prepareQuery(String sql, MySqlConnection connection) {
        CancellationToken cancelSupport = new CancellationToken();
        DefaultDbSessionFuture<PreparedQuery> future = new DefaultDbSessionFuture<PreparedQuery>(connection,cancelSupport);

        return new MySqlRequest("Prepare-Query: "+sql,future,
                new ExpectPreparQuery(future),
                new StringCommandRequest(Command.STATEMENT_PREPARE,sql,cancelSupport));
    }

    public static MySqlRequest closeStatemeent(StatementPreparedEOF statementInfo, MySqlConnection connection) {
        DefaultDbFuture<Void> future = new DefaultDbFuture<Void>();
        future.setResult(null);
        return new MySqlRequest("Close-Statement: ",future,
                new AcceptNextResponse(connection),
                new ClosePreparedStatementRequest(statementInfo.getHandlerId()));
    }

    public static MySqlRequest beginTransaction(MySqlConnection connection) {
        DefaultDbSessionFuture<Result> future = new DefaultDbSessionFuture<Result>(connection);
        return new MySqlRequest("Begin-Transaction: ",future,
                new ExpectUpdateResult(future),
                new StringCommandRequest(Command.QUERY, "begin",CancellationToken.NO_CANCELLATION));
    }

    public static MySqlRequest commitTransaction(MySqlConnection connection) {
        DefaultDbSessionFuture<Result> future = new DefaultDbSessionFuture<Result>(connection);
        return new MySqlRequest("Commit-Transaction: ",future,
                new ExpectUpdateResult(future),
                new StringCommandRequest(Command.QUERY, "commit",CancellationToken.NO_CANCELLATION));
    }
    public static MySqlRequest rollbackTransaction(MySqlConnection connection) {
        DefaultDbSessionFuture<Result> future = new DefaultDbSessionFuture<Result>(connection);
        return new MySqlRequest("Rollback-Transaction: ",future,
                new ExpectUpdateResult(future),
                new StringCommandRequest(Command.QUERY, "rollback",CancellationToken.NO_CANCELLATION));
    }
}
