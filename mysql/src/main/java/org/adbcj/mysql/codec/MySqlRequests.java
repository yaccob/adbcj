package org.adbcj.mysql.codec;

import org.adbcj.Result;
import org.adbcj.ResultHandler;
import org.adbcj.mysql.codec.decoding.ExpectOK;
import org.adbcj.mysql.codec.decoding.ExpectQueryResult;
import org.adbcj.mysql.codec.decoding.ExpectUpdateResult;
import org.adbcj.mysql.codec.decoding.Row;
import org.adbcj.mysql.codec.packets.Command;
import org.adbcj.mysql.codec.packets.CommandRequest;
import org.adbcj.mysql.codec.packets.StringCommandRequest;
import org.adbcj.support.*;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class MySqlRequests {
    public static MySqlRequest createCloseRequest(MySqlConnection connection) {
        DefaultDbFuture<Void> future = new DefaultDbFuture<Void>();

        return new MySqlRequest("Close",future, new ExpectOK<Void>(future, connection),new CommandRequest(Command.QUIT));
    }
    public static <T> MySqlRequest executeQuery(String query, ResultHandler<T> eventHandler, T accumulator, MySqlConnection connection) {
        DefaultDbSessionFuture<T> future = new DefaultDbSessionFuture<T>(connection);
        ResultHandler<T> handleFailures = SafeResultHandlerDecorator.wrap(eventHandler, future);
        return new MySqlRequest("Query: "+query,future,
                new ExpectQueryResult<T>(Row.RowDecodingType.STRING_BASED, future, handleFailures,accumulator),
                new StringCommandRequest(Command.QUERY,query));
    }

    public static MySqlRequest executeUpdate(String sql, MySqlConnection connection) {
        DefaultDbSessionFuture<Result> future = new DefaultDbSessionFuture<Result>(connection);
        return new MySqlRequest("Update: "+sql,future,
                new ExpectUpdateResult(future),
                new StringCommandRequest(Command.QUERY,sql));
    }
}
