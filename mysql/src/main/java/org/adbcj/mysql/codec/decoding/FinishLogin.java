package org.adbcj.mysql.codec.decoding;

import org.adbcj.Connection;
import org.adbcj.mysql.codec.MySqlConnection;
import org.adbcj.mysql.codec.packets.ErrorResponse;
import org.adbcj.mysql.codec.packets.OkResponse;
import org.adbcj.support.DefaultDbFuture;

/**
 * @author roman.stoffel@gamlor.info
 */
public class FinishLogin extends ResponseStart {
    private final DefaultDbFuture<Connection> futureToComplete;

    public FinishLogin(DefaultDbFuture<Connection> futureToComplete, MySqlConnection connectionToBuild) {
        super(connectionToBuild);
        this.futureToComplete = futureToComplete;
    }

    @Override
    protected ResultAndState handleError(ErrorResponse errorResponse) {
        futureToComplete.trySetException(errorResponse.toException());
        return new ResultAndState(acceptNextResponse(),errorResponse);
    }

    @Override
    protected ResultAndState handleOk(OkResponse.RegularOK regularOK) {
        futureToComplete.trySetResult(connection);
        return new ResultAndState(acceptNextResponse(),regularOK);
    }

    protected AcceptNextResponse acceptNextResponse() {
        return new AcceptNextResponse(connection);
    }
}
