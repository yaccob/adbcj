package org.adbcj.mysql.codec.decoding;

import org.adbcj.ResultHandler;
import org.adbcj.mysql.codec.MySqlConnection;
import org.adbcj.mysql.codec.packets.OkResponse;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.OneArgFunction;

/**
 * @author roman.stoffel@gamlor.info
 */
public class ExpectStatementResult extends ExpectQueryResult {

    public ExpectStatementResult(Row.RowDecodingType decodingType,
                                 DefaultDbFuture future,
                                 MySqlConnection connection,
                                 ResultHandler eventHandler,
                                 Object accumulator) {
        super(decodingType, future,connection, eventHandler, accumulator);
    }


    @Override
    protected ResultAndState handleOk(OkResponse.RegularOK regularOK) {
        return ExpectUpdateResult.handleUpdateResult(regularOK, future,connection, OneArgFunction.ID_FUNCTION);
    }
}
