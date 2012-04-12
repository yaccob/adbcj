package org.adbcj.mysql.codec;

import org.adbcj.Field;
import org.adbcj.PreparedStatement;
import org.adbcj.Value;
import org.adbcj.mysql.codec.packets.*;
import org.adbcj.support.DefaultValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author roman.stoffel@gamlor.info
 * @since 12.04.12
 */
public abstract class DecoderState {
    private static final Logger logger = LoggerFactory.getLogger(DecoderState.class);
    private static final String CHARSET = "UTF8";
    public static final int RESPONSE_EOF = 0xfe;

    public static final DecoderState CONNECTING = new Connecting();
    public static final DecoderState RESPONSE = new Response();
    private static DecoderState FIELD(int expectedAmountOfFields, List<MysqlField> fields){
        return new FieldDecodingState(expectedAmountOfFields,fields);

    }
    private static DecoderState FIELD_EOF(List<MysqlField> fields){
        return new FieldEof(fields);
    }
    private static DecoderState ROW(List<MysqlField> fields){
        return new Row(fields);

    }
    static class Connecting extends DecoderState{
        /**
         * The salt size in a server greeting
         */
        public static final int SALT_SIZE = 8;

        /**
         * The size of the second salt in a server greeting
         */
        public static final int SALT2_SIZE = 12;

        /**
         * Number of unused bytes in server greeting
         */
        public static final int GREETING_UNUSED_SIZE = 13;

        @Override
        public ResultAndState parse(int length,
                                  int packetNumber,
                                  BoundedInputStream in,
                                  AbstractMySqlConnection connection) throws IOException{
            ServerGreeting serverGreeting = decodeServerGreeting(in, length, packetNumber);
            return result(RESPONSE,serverGreeting);
        }

        protected ServerGreeting decodeServerGreeting(InputStream in, int length, int packetNumber) throws IOException {
            int protocol = IoUtils.safeRead(in);
            String version = IoUtils.readString(in, "ASCII");
            int threadId = IoUtils.readInt(in);

            byte[] salt = new byte[SALT_SIZE + SALT2_SIZE];
            in.read(salt, 0, SALT_SIZE);
            in.read(); // Throw away 0 byte

            Set<ClientCapabilities> serverCapabilities = IoUtils.readEnumSetShort(in, ClientCapabilities.class);
            MysqlCharacterSet charSet = MysqlCharacterSet.findById(in.read());
            Set<ServerStatus> serverStatus = IoUtils.readEnumSetShort(in, ServerStatus.class);
            in.skip(GREETING_UNUSED_SIZE);

            in.read(salt, SALT_SIZE, SALT2_SIZE);
            in.read(); // Throw away 0 byte

            return new ServerGreeting(length, packetNumber, protocol, version, threadId, salt, serverCapabilities, charSet,
                    serverStatus);
        }
    }

    static class Response extends DecoderState{
        public static final int RESPONSE_OK = 0x00;
        public static final int RESPONSE_ERROR = 0xff;
        @Override
        public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, AbstractMySqlConnection connection) throws IOException {

            int fieldCount = in.read();
            if (fieldCount == RESPONSE_OK) {
                // Create Ok response
                return result(RESPONSE, decodeOkResponse(connection, in, length, packetNumber));
            }
            if (fieldCount == RESPONSE_ERROR) {
                // Create error response
                return result(RESPONSE,decodeErrorResponse(in, length, packetNumber));
            }
            if (fieldCount == RESPONSE_EOF) {
                throw new IllegalStateException("Did not expect an EOF response from the server");
            }
            // Must be receiving result set header

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


        protected OkResponse decodeOkResponse(AbstractMySqlConnection connection, BoundedInputStream in, int length, int packetNumber) throws IOException {
            if (connection!=null && connection.<PreparedStatement>getActiveRequest() instanceof AbstractMySqlConnection.PreparedStatementRequest) {
                return OkResponse.interpretAsPreparedStatement(length, packetNumber, in);
            }
            return OkResponse.interpretAsRegularOk(length, packetNumber, in);
        }

        protected ErrorResponse decodeErrorResponse(InputStream in, int length, int packetNumber) throws IOException {
            int errorNumber = IoUtils.readUnsignedShort(in);
            in.read(); // Throw away sqlstate marker
            String sqlState = IoUtils.readString(in, CHARSET);
            String message = IoUtils.readString(in, CHARSET);
            return new ErrorResponse(length, packetNumber, errorNumber, sqlState, message);
        }
    }
    static class FieldDecodingState extends DecoderState{
        private final int expectedFieldPackets;
        private final List<MysqlField> fields;

        FieldDecodingState(int expectedFieldPackets, List<MysqlField> fields) {
            this.expectedFieldPackets = expectedFieldPackets;
            this.fields = fields;
        }

        @Override
        public ResultAndState parse(int length,
                                    int packetNumber,
                                    BoundedInputStream in, AbstractMySqlConnection connection) throws IOException {
            Tuple<ResultSetFieldResponse,List<MysqlField>> resultSetFieldResponse = decodeFieldResponse(in, length, packetNumber);

            int restOfExpectedFields = expectedFieldPackets-1;
            logger.trace("fieldPacketCount: {}", expectedFieldPackets);
            if (restOfExpectedFields == 0) {
                return result(FIELD_EOF(resultSetFieldResponse.getSecond()),resultSetFieldResponse.getFirst());
            } else{
                return result(FIELD(restOfExpectedFields,resultSetFieldResponse.getSecond()),resultSetFieldResponse.getFirst());
            }
        }


        private Tuple<ResultSetFieldResponse,List<MysqlField>> decodeFieldResponse(InputStream in,
                                                           int packetLength, int packetNumber) throws IOException {
            String catalogName = IoUtils.readLengthCodedString(in, CHARSET);
            String schemaName = IoUtils.readLengthCodedString(in, CHARSET);
            String tableLabel = IoUtils.readLengthCodedString(in, CHARSET);
            String tableName = IoUtils.readLengthCodedString(in, CHARSET);
            String columnLabel = IoUtils.readLengthCodedString(in, CHARSET);
            String columnName = IoUtils.readLengthCodedString(in, CHARSET);
            in.read(); // Skip filler
            int characterSetNumber = IoUtils.readUnsignedShort(in);
            MysqlCharacterSet charSet = MysqlCharacterSet.findById(characterSetNumber);
            long length = IoUtils.readUnsignedInt(in);
            int fieldTypeId = in.read();
            MysqlType fieldType = MysqlType.findById(fieldTypeId);
            Set<FieldFlag> flags = IoUtils.readEnumSet(in, FieldFlag.class);
            int decimals = in.read();
            in.skip(2); // Skip filler
            long fieldDefault = IoUtils.readBinaryLengthEncoding(in);
            int fieldIndex = fields.size();
            MysqlField field = new MysqlField(fieldIndex, catalogName, schemaName, tableLabel, tableName, fieldType, columnLabel,
                    columnName, 0, // Figure out precision
                    decimals, charSet, length, flags, fieldDefault);
            List<MysqlField> accumulatedFields = new ArrayList<MysqlField>(fields);
            accumulatedFields.add(field);
            return Tuple.create(new ResultSetFieldResponse(packetLength, packetNumber, field),accumulatedFields);
        }
    }
    private static class FieldEof extends DecoderState{
        private final List<MysqlField> fields;

        public FieldEof(List<MysqlField> fields) {
            this.fields = fields;
        }

        @Override
        public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, AbstractMySqlConnection connection) throws IOException {
            int fieldCount = in.read();

            if (fieldCount != RESPONSE_EOF) {
                throw new IllegalStateException("Expected an EOF response from the server");
            }
            EofResponse fieldEof = decodeEofResponse(in, length, packetNumber, EofResponse.Type.FIELD);
            return result(ROW(fields),fieldEof);
        }


    }
    private static class Row extends DecoderState{
        private final List<MysqlField> fields;

        public Row(List<MysqlField> fields) {
            this.fields = fields;
        }

        @Override
        public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, AbstractMySqlConnection connection) throws IOException {
            int fieldCount = in.read(); // This is only for checking for EOF
            if (fieldCount == RESPONSE_EOF) {
                EofResponse rowEof = decodeEofResponse(in, length, packetNumber, EofResponse.Type.ROW);

                return result(RESPONSE,rowEof);
            }

            Value[] values = new Value[fields.size()];
            int i = 0;
            for (Field field : fields  ) {
                Object value = null;
                if (fieldCount != IoUtils.NULL_VALUE) {
                    // We will have to move this as some datatypes will not be sent across the wire as strings
                    String strVal = IoUtils.readLengthCodedString(in, fieldCount, CHARSET);

                    // TODO add decoding for all column types
                    switch (field.getColumnType()) {
                        case TINYINT:
                            value = Byte.valueOf(strVal);
                            break;
                        case INTEGER:
                        case BIGINT:
                            value = Long.valueOf(strVal);
                            break;
                        case VARCHAR:
                            value = strVal;
                            break;
                        default:
                            throw new IllegalStateException("Don't know how to handle column type of "
                                    + field.getColumnType());
                    }
                }
                values[field.getIndex()] = new DefaultValue(field, value);
                i++;
                if (i < fields.size()) {
                    fieldCount = in.read();
                }
            }
            return result(ROW(fields),new ResultSetRowResponse(length, packetNumber, values));

        }
    }


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