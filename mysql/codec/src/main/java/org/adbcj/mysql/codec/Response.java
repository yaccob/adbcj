package org.adbcj.mysql.codec;

import org.adbcj.PreparedStatement;
import org.adbcj.mysql.codec.packets.ErrorResponse;
import org.adbcj.mysql.codec.packets.OkResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Set;

/**
* @author roman.stoffel@gamlor.info
* @since 12.04.12
*/
class Response extends DecoderState{
    public static final int RESPONSE_OK = 0x00;
    public static final int RESPONSE_ERROR = 0xff;
    @Override
    public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, AbstractMySqlConnection connection) throws IOException {

        int fieldCount = in.read();
        if (fieldCount == RESPONSE_OK) {
            if (connection!=null && connection.<PreparedStatement>getActiveRequest() instanceof AbstractMySqlConnection.PreparedStatementRequest) {
                final OkResponse.PreparedStatementOK statementOK = OkResponse.interpretAsPreparedStatement(length, packetNumber, in);
                if(statementOK.getParams()>0){
                    return result(new DecoderState() {
                        @Override
                        public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, AbstractMySqlConnection connection) throws IOException {
                            String catalogName = IoUtils.readLengthCodedString(in, CHARSET);
                            String schemaName = IoUtils.readLengthCodedString(in, CHARSET);
                            String tableLabel = IoUtils.readLengthCodedString(in, CHARSET);
                            String tableName = IoUtils.readLengthCodedString(in, CHARSET);
                            String columnLabel = IoUtils.readLengthCodedString(in, CHARSET);
                            String columnName = IoUtils.readLengthCodedString(in, CHARSET);
                            in.read(); // Skip filler
                            int characterSetNumber = IoUtils.readUnsignedShort(in);
                            MysqlCharacterSet charSet = MysqlCharacterSet.findById(characterSetNumber);
                            long length2 = IoUtils.readUnsignedInt(in);
                            int fieldTypeId = in.read();
                            MysqlType fieldType = MysqlType.findById(fieldTypeId);
                            Set<FieldFlag> flags = IoUtils.readEnumSet(in, FieldFlag.class);
                            int decimals = in.read();
                            in.skip(2); // Skip filler
                            long fieldDefault = IoUtils.readBinaryLengthEncoding(in);
//                            int fieldIndex = fields.size();
//                            MysqlField field = new MysqlField(fieldIndex, catalogName, schemaName, tableLabel, tableName, fieldType, columnLabel,
//                                    columnName, 0, // Figure out precision
//                                    decimals, charSet, length, flags, fieldDefault);
//                            List<MysqlField> accumulatedFields = new ArrayList<MysqlField>(fields);
//                            accumulatedFields.add(field);
                            return result(EOF_EXPECTED,statementOK);
                        }
                    }, statementOK);
                } else{
                    return result(EOF_EXPECTED,statementOK);
                }
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
