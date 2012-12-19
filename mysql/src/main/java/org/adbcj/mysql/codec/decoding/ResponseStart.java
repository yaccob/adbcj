package org.adbcj.mysql.codec.decoding;

import org.adbcj.mysql.codec.BoundedInputStream;
import org.adbcj.mysql.codec.IoUtils;
import org.adbcj.mysql.codec.MySqlConnection;
import org.adbcj.mysql.codec.packets.ErrorResponse;
import org.adbcj.mysql.codec.packets.OkResponse;
import org.jboss.netty.channel.Channel;

import java.io.IOException;
import java.io.InputStream;

/**
* @author roman.stoffel@gamlor.info
* @since 12.04.12
*/
public abstract class ResponseStart extends DecoderState {
    public static final int RESPONSE_OK = 0x00;
    public static final int RESPONSE_ERROR = 0xff;
    protected final MySqlConnection connection;

    protected ResponseStart(MySqlConnection connection) {
        this.connection = connection;
    }

    @Override
    public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, Channel channel) throws IOException {

        int fieldCount = in.read();
        if (fieldCount == RESPONSE_OK) {
            return handleOk(OkResponse.interpretAsRegularOk(length, packetNumber, in));
        }
        if (fieldCount == RESPONSE_ERROR) {
            return handleError(decodeErrorResponse(in, length, packetNumber));
        }
        if (fieldCount == RESPONSE_EOF) {
            throw new IllegalStateException("Did not expect an EOF response from the server");
        }
        return parseAsResult(length, packetNumber, in, fieldCount);
    }

    protected abstract ResultAndState handleError(ErrorResponse errorResponse);

    protected abstract ResultAndState handleOk(OkResponse.RegularOK regularOK);

    @Override
    public String toString() {
        return "RESPONSE";
    }

    private ResultAndState parseAsResult(int length, int packetNumber, BoundedInputStream in, int fieldCount) throws IOException {
        throw new Error("TODO");
//        // Get the number of fields. The largest this can be is a 24-bit
//        // integer so cast to int is ok
//        int expectedFieldPackets = (int) IoUtils.readBinaryLengthEncoding(in, fieldCount);
//        logger.trace("Field count {}", expectedFieldPackets);
//
//        Long extra = null;
//        if (in.getRemaining() > 0) {
//            extra = IoUtils.readBinaryLengthEncoding(in);
//        }
//
//        return result(FIELD(expectedFieldPackets,new ArrayList<MysqlField>()),new ResultSetResponse(length, packetNumber, expectedFieldPackets, extra));
    }


    protected ErrorResponse decodeErrorResponse(InputStream in, int length, int packetNumber) throws IOException {
        int errorNumber = IoUtils.readUnsignedShort(in);
        in.read(); // Throw away sqlstate marker
        String sqlState = IoUtils.readNullTerminatedString(in, CHARSET);
        String message = IoUtils.readNullTerminatedString(in, CHARSET);
        return new ErrorResponse(length, packetNumber, errorNumber, sqlState, message);
    }
}
