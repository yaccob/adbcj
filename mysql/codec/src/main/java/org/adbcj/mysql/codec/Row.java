package org.adbcj.mysql.codec;

import org.adbcj.Field;
import org.adbcj.Value;
import org.adbcj.mysql.codec.packets.EofResponse;
import org.adbcj.mysql.codec.packets.ResultSetRowResponse;
import org.adbcj.support.DefaultValue;

import java.io.IOException;
import java.util.List;

/**
* @author roman.stoffel@gamlor.info
* @since 12.04.12
*/
class Row extends DecoderState{
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
