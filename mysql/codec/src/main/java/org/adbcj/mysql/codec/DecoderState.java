package org.adbcj.mysql.codec;

import org.adbcj.mysql.codec.packets.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

/**
 * @author roman.stoffel@gamlor.info
 * @since 12.04.12
 */
public abstract class DecoderState {
    protected static final Logger logger = LoggerFactory.getLogger(DecoderState.class);
    protected static final String CHARSET = "UTF8";
    public static final int RESPONSE_EOF = 0xfe;

    public static final DecoderState CONNECTING = new Connecting();
    public static final DecoderState RESPONSE = new Response();
    protected static DecoderState FIELD(int expectedAmountOfFields, List<MysqlField> fields){
        return new FieldDecodingState(expectedAmountOfFields,fields);

    }
    protected static DecoderState FIELD_EOF(List<MysqlField> fields){
        return new FieldEof(fields);
    }
    protected static DecoderState ROW(List<MysqlField> fields){
        return new Row(fields);
    }
    protected static DecoderState EOF_EXPECTED = new ExpectEOF();


    public abstract ResultAndState parse(int length,
                                       int packetNumber,
                                       BoundedInputStream in,
                                       AbstractMySqlConnection connection) throws IOException;


    public ResultAndState result( DecoderState newState,ServerPacket result){
        return new ResultAndState(newState,result);
    }
    protected EofResponse decodeEofResponse(InputStream in, int length, int packetNumber, EofResponse.Type type) throws IOException {
        int warnings = IoUtils.readUnsignedShort(in);
        Set<ServerStatus> serverStatus = IoUtils.readEnumSetShort(in, ServerStatus.class);

        return new EofResponse(length, packetNumber, warnings, serverStatus, type);
    }
}


class ResultAndState{
    private final ServerPacket result;
    private final DecoderState newState;

    ResultAndState(DecoderState newState,ServerPacket result) {
        this.result = result;
        this.newState = newState;
    }

    public ServerPacket getResult() {
        return result;
    }

    public DecoderState getNewState() {
        return newState;
    }
}