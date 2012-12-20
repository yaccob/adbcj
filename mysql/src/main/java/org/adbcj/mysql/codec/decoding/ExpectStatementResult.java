package org.adbcj.mysql.codec.decoding;

import org.adbcj.ResultHandler;
import org.adbcj.mysql.codec.packets.OkResponse;
import org.adbcj.support.DefaultDbSessionFuture;

/**
 * @author roman.stoffel@gamlor.info
 */
public class ExpectStatementResult extends ExpectQueryResult {

    public ExpectStatementResult(Row.RowDecodingType decodingType,
                                 DefaultDbSessionFuture future,
                                 ResultHandler eventHandler, Object accumulator) {
        super(decodingType, future, eventHandler, accumulator);
    }


    @Override
    protected ResultAndState handleOk(OkResponse.RegularOK regularOK) {
        return ExpectUpdateResult.handleUpdateResult(regularOK, future);
    }
}
