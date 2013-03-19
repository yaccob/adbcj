package org.adbcj.mysql.codec;

import org.adbcj.PreparedQuery;
import org.adbcj.Result;
import org.adbcj.ResultHandler;
import org.adbcj.mysql.codec.decoding.*;
import org.adbcj.mysql.codec.packets.*;
import org.adbcj.support.*;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class MySqlRequests {
    private static final OneArgFunction<MysqlResult,Void> TO_VOID = new OneArgFunction<MysqlResult,Void>() {
        @Override
        public Void apply(MysqlResult arg) {
            return null;
        }
    };

    public static MySqlRequest createCloseRequest(MySqlConnection connection) {
        DefaultDbFuture<Void> future = new DefaultDbFuture<Void>(connection.stackTraceOptions());

        return new MySqlRequest("Close",future, new ExpectOK<Void>(future, connection),new CommandRequest(Command.QUIT));
    }
    public static <T> MySqlRequest executeQuery(String query, ResultHandler<T> eventHandler, T accumulator, MySqlConnection connection) {
        CancellationToken cancelSupport = new CancellationToken();
        DefaultDbFuture<T> future = new DefaultDbFuture<T>(connection.stackTraceOptions(),cancelSupport);
        ResultHandler<T> handleFailures = SafeResultHandlerDecorator.wrap(eventHandler, future);
        return new MySqlRequest("Query: "+query,future,
                new ExpectQueryResult<T>(Row.RowDecodingType.STRING_BASED, future,connection, handleFailures,accumulator),
                new StringCommandRequest(Command.QUERY,query,cancelSupport));
    }
    public static <T> MySqlRequest executePreparedQuery(StatementPreparedEOF stmp,
                                                        Object[] data,
                                                        ResultHandler<T> eventHandler,
                                                        T accumulator, MySqlConnection connection) {
        DefaultDbFuture<T> future = new DefaultDbFuture<T>(connection.stackTraceOptions());
        ResultHandler<T> handleFailures = SafeResultHandlerDecorator.wrap(eventHandler, future);
        return new MySqlRequest("Execute-Statement",future,
                new ExpectStatementResult(Row.RowDecodingType.BINARY, future,connection, handleFailures,accumulator),
                new PreparedStatementRequest(stmp.getHandlerId(),stmp.getParametersTypes(),data));
    }

    public static MySqlRequest executeUpdate(String sql, MySqlConnection connection) {
        CancellationToken cancelSupport = new CancellationToken();
        DefaultDbFuture<Result> future = new DefaultDbFuture<Result>(connection.stackTraceOptions(),cancelSupport);
        return new MySqlRequest("Update: "+sql,future,
                new ExpectUpdateResult(future,connection),
                new StringCommandRequest(Command.QUERY,sql,cancelSupport));
    }

    public static MySqlRequest prepareQuery(String sql, MySqlConnection connection) {
        CancellationToken cancelSupport = new CancellationToken();
        DefaultDbFuture<PreparedQuery> future = new DefaultDbFuture<PreparedQuery>(connection.stackTraceOptions(),cancelSupport);

        return new MySqlRequest("Prepare-Query: "+sql,future,
                new ExpectPreparQuery(future,connection),
                new StringCommandRequest(Command.STATEMENT_PREPARE,sql,cancelSupport));
    }

    public static MySqlRequest closeStatemeent(StatementPreparedEOF statementInfo, MySqlConnection connection) {
        DefaultDbFuture<Void> future = new DefaultDbFuture<Void>(connection.stackTraceOptions());
        future.setResult(null);
        return new MySqlRequest("Close-Statement: ",future,
                new AcceptNextResponse(connection),
                new ClosePreparedStatementRequest(statementInfo.getHandlerId()));
    }

    public static MySqlRequest beginTransaction(MySqlConnection connection) {
        DefaultDbFuture<Result> future = new DefaultDbFuture<Result>(connection.stackTraceOptions());
        return new MySqlRequest("Begin-Transaction: ",future,
                new ExpectUpdateResult(future,connection),
                new StringCommandRequest(Command.QUERY, "begin",CancellationToken.NO_CANCELLATION));
    }

    public static MySqlRequest commitTransaction(MySqlConnection connection) {
        DefaultDbFuture<Result> future = new DefaultDbFuture<Result>(connection.stackTraceOptions());
        return new MySqlRequest("Commit-Transaction: ",future,
                new ExpectUpdateResult(future,connection,TO_VOID),
                new StringCommandRequest(Command.QUERY, "commit",CancellationToken.NO_CANCELLATION));
    }
    public static MySqlRequest rollbackTransaction(MySqlConnection connection) {
        DefaultDbFuture<Result> future = new DefaultDbFuture<Result>(connection.stackTraceOptions());
        return new MySqlRequest("Rollback-Transaction: ",future,
                new ExpectUpdateResult(future,connection,TO_VOID),
                new StringCommandRequest(Command.QUERY, "rollback",CancellationToken.NO_CANCELLATION));
    }
}
