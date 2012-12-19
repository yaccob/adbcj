package org.adbcj.mysql.codec.decoding;

import org.adbcj.ResultHandler;
import org.adbcj.mysql.codec.MySqlConnection;
import org.adbcj.mysql.codec.packets.ErrorResponse;
import org.adbcj.mysql.codec.packets.OkResponse;
import org.adbcj.support.DefaultDbSessionFuture;

/**
 * @author roman.stoffel@gamlor.info
 */
public class ExpectQueryResult<T> extends ResponseStart {
    private final ResultHandler<T> eventHandler;
    private final T accumulator;

    public ExpectQueryResult(DefaultDbSessionFuture<T> future, ResultHandler<T> eventHandler,T  accumulator) {
        super((MySqlConnection) future.getSession());
        this.eventHandler = eventHandler;
        this.accumulator = accumulator;
    }

    @Override
    protected ResultAndState handleError(ErrorResponse errorResponse) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    protected ResultAndState handleOk(OkResponse.RegularOK regularOK) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }
}
