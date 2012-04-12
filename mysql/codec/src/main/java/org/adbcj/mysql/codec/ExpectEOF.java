package org.adbcj.mysql.codec;

import org.adbcj.mysql.codec.packets.EofResponse;

import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 * @since 12.04.12
 */
class ExpectEOF extends DecoderState {


    @Override
    public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, AbstractMySqlConnection connection) throws IOException {
        if (in.read() == RESPONSE_EOF) {

            EofResponse eof = decodeEofResponse(in, length, packetNumber, EofResponse.Type.STATEMENT);
            return result(RESPONSE,eof);
        } else{
            throw new IllegalStateException("Did not expect a EOF from the server");
        }
    }
}
