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
        if(connection.getActiveRequest() instanceof MySqlPreparedStatement.ExecutePrepareStatement){
            binaryDecode(in, values);
        } else{
            stringDecode(in, fieldCount, values);
        }
        return result(ROW(fields),new ResultSetRowResponse(length, packetNumber, values));

    }

    private void binaryDecode(BoundedInputStream in, Value[] values) throws IOException {
        // 0 (packet header)   should have been read by the calling method
        byte[] nullBits = new byte[ (values.length+7+2)/8];
        in.read(nullBits);
        for (MysqlField field : fields  ) {
            Object value = null;
            if(hasValue(field.getIndex(), nullBits)){
                switch (field.getMysqlType()) {
                    case LONG:
                        value =  IoUtils.readInt(in);
                        break;
                    case VAR_STRING:
                        value =  IoUtils.readLengthCodedString(in, in.read(), CHARSET);
                        break;
                    case NULL:
                        value =  null;
                        break;
                    default:
                        throw new IllegalStateException("Not yet implemented");
                }

            }

            values[field.getIndex()] = new DefaultValue(field, value);
        }

    }


    private boolean hasValue(int valuePos, byte[] nullBitMap) {
        int bit = 4; // first two bits are reserved for future use
        int nullMaskPos = 0;
        boolean hasValue = false;
        for (int i = 0; i <= valuePos; i++) {
            if((nullBitMap[nullMaskPos] & bit) > 0) {
                hasValue = false;
            } else{
                hasValue = true;
            }
            if (((bit <<= 1) & 255) == 0) {
                bit = 1;
                nullMaskPos++;
            }
        }
        return hasValue;
    }

    private void stringDecode(BoundedInputStream in, int fieldCount, Value[] values) throws IOException {
        int i=0;
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
                        value = Integer.valueOf(strVal);
                        break;
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
    }
}
