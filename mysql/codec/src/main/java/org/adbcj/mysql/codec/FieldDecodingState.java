package org.adbcj.mysql.codec;

import org.adbcj.mysql.codec.packets.ResultSetFieldResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
* @author roman.stoffel@gamlor.info
* @since 12.04.12
*/
class FieldDecodingState extends DecoderState{
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
