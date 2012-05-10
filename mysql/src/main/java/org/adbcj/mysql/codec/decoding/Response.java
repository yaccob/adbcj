package org.adbcj.mysql.codec.decoding;

import org.adbcj.PreparedStatement;
import org.adbcj.mysql.codec.*;
import org.adbcj.mysql.codec.decoding.DecoderState;
import org.adbcj.mysql.codec.packets.ErrorResponse;
import org.adbcj.mysql.codec.packets.OkResponse;
import org.adbcj.mysql.codec.packets.PreparedStatementToBuild;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

/**
* @author roman.stoffel@gamlor.info
* @since 12.04.12
*/
class Response extends DecoderState {
    public static final int RESPONSE_OK = 0x00;
    public static final int RESPONSE_ERROR = 0xff;
    @Override
    public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, AbstractMySqlConnection connection) throws IOException {

        int fieldCount = in.read();
        if (fieldCount == RESPONSE_OK) {
            if (connection!=null && connection.<PreparedStatement>getActiveRequest() instanceof AbstractMySqlConnection.PreparedStatementRequest) {
                return processOKMessage(length, packetNumber, in);
            }
            return result(RESPONSE, OkResponse.interpretAsRegularOk(length, packetNumber, in));
        }
        if (fieldCount == RESPONSE_ERROR) {
            return result(RESPONSE,decodeErrorResponse(in, length, packetNumber));
        }
        if (fieldCount == RESPONSE_EOF) {
            throw new IllegalStateException("Did not expect an EOF response from the server");
        }
        return parseAsResult(length, packetNumber, in, fieldCount);
    }

    private ResultAndState processOKMessage(int length, int packetNumber, BoundedInputStream in) throws IOException {
        final OkResponse.PreparedStatementOK statementOK = OkResponse.interpretAsPreparedStatement(length, packetNumber, in);
        return result(FINISH_PREPARE_STATEMENT_OK(new PreparedStatementToBuild(length, packetNumber, statementOK)), statementOK);
    }

    private ResultAndState parseAsResult(int length, int packetNumber, BoundedInputStream in, int fieldCount) throws IOException {
        // Get the number of fields. The largest this can be is a 24-bit
        // integer so cast to int is ok
        int expectedFieldPackets = (int) IoUtils.readBinaryLengthEncoding(in, fieldCount);
        logger.trace("Field count {}", expectedFieldPackets);

        Long extra = null;
        if (in.getRemaining() > 0) {
            extra = IoUtils.readBinaryLengthEncoding(in);
        }

        return result(FIELD(expectedFieldPackets,new ArrayList<MysqlField>()),new ResultSetResponse(length, packetNumber, expectedFieldPackets, extra));
    }


    protected ErrorResponse decodeErrorResponse(InputStream in, int length, int packetNumber) throws IOException {
        int errorNumber = IoUtils.readUnsignedShort(in);
        in.read(); // Throw away sqlstate marker
        String sqlState = IoUtils.readString(in, CHARSET);
        String message = IoUtils.readString(in, CHARSET);
        return new ErrorResponse(length, packetNumber, errorNumber, sqlState, message);
    }
}
