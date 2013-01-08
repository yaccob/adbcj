package org.adbcj.mysql.codec.decoding;

import org.adbcj.mysql.codec.MySqlConnection;
import org.adbcj.mysql.codec.packets.ErrorResponse;
import org.adbcj.mysql.codec.packets.OkResponse;
import org.adbcj.support.DefaultDbFuture;

/**
 * @author roman.stoffel@gamlor.info
 */
public class ExpectOK<T> extends ResponseStart {

    protected final DefaultDbFuture<T> futureToComplete;

    public ExpectOK(DefaultDbFuture<T> futureToComplete,MySqlConnection connection) {
        super(connection);
        this.futureToComplete = futureToComplete;
    }
    @Override
    protected ResultAndState handleError(ErrorResponse errorResponse) {
        futureToComplete.trySetException(errorResponse.toException());
        return new ResultAndState(new AcceptNextResponse(connection), errorResponse);
    }

    @Override
    protected ResultAndState handleOk(OkResponse.RegularOK regularOK) {
        this.futureToComplete.trySetResult(null);
        return new ResultAndState(new AcceptNextResponse(connection), regularOK);
    }
}
