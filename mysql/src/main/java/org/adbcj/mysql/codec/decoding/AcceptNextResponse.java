package org.adbcj.mysql.codec.decoding;

import org.adbcj.mysql.codec.MySqlConnection;
import org.adbcj.mysql.codec.packets.ErrorResponse;
import org.adbcj.mysql.codec.packets.OkResponse;

/**
 * @author roman.stoffel@gamlor.info
 */
public class AcceptNextResponse extends ResponseStart {
    public AcceptNextResponse(MySqlConnection connection) {
        super(connection);
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
