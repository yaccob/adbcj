package org.adbcj.mysql.codec.decoding;

import org.adbcj.Field;
import org.adbcj.Value;
import org.adbcj.mysql.codec.*;
import org.adbcj.mysql.codec.packets.EofResponse;
import org.adbcj.mysql.codec.packets.ResultSetRowResponse;
import org.adbcj.support.DefaultValue;
import org.jboss.netty.channel.Channel;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.List;

/**
* @author roman.stoffel@gamlor.info
* @since 12.04.12
*/
class Row extends DecoderState {
    private final List<MysqlField> fields;

    public Row(List<MysqlField> fields) {
        this.fields = fields;
    }

    @Override
    public ResultAndState parse(int length, int packetNumber,
                                BoundedInputStream in, Channel channel) throws IOException {
        int fieldCount = in.read(); // This is only for checking for EOF
        if (fieldCount == RESPONSE_EOF) {
            EofResponse rowEof = decodeEofResponse(in, length, packetNumber, EofResponse.Type.ROW);
             throw new Error("TODO Accept next request");
//            return result(RESPONSE,rowEof);
        }

        Value[] values = new Value[fields.size()];
        if(decideDecodingAccordingToState()){
            binaryDecode(in, values);
        } else{
            try {
                stringDecode(in, fieldCount, values);
            } catch (ParseException e) {
                throw new RuntimeException(e.getMessage(),e);
            }
        }
        return result(ROW(fields),new ResultSetRowResponse(length, packetNumber, values));

    }

    private boolean decideDecodingAccordingToState() {
        throw new Error("TODO");
    }

    @Override
    public String toString() {
        return "ROW";
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
                    case LONGLONG:
                        value =  IoUtils.readLong(in);
                        break;
                    case VAR_STRING:
                        value =  IoUtils.readLengthCodedString(in, in.read(), CHARSET);
                        break;
                    case NEWDECIMAL:
                        value =  IoUtils.readLengthCodedString(in, in.read(), CHARSET);
                        break;
                    case DATE:
                        value =  IoUtils.readDate(in);
                        break;
                    case DATETIME:
                        value =  IoUtils.readDate(in);
                        break;
                    case TIME:
                        value =  IoUtils.readDate(in);
                        break;
                    case TIMESTAMP:
                        value =  IoUtils.readDate(in);
                        break;
                    case DOUBLE:
                        value =  Double.longBitsToDouble(IoUtils.readLong(in));
                        break;
                    case BLOB:
                        value =  IoUtils.readLengthCodedString(in, in.read(), CHARSET);
                        break;
                    case NULL:
                        value =  null;
                        break;
                    default:
                        throw new IllegalStateException("Not yet implemented for type "+field.getMysqlType());
                }

            }

            values[field.getIndex()] = new DefaultValue(value);
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

    private void stringDecode(BoundedInputStream in, int fieldCount, Value[] values) throws IOException,ParseException {
        int i=0;
        for (Field field : fields  ) {
            Object value = null;
            if (fieldCount != IoUtils.NULL_VALUE) {
                // We will have to move this as some datatypes will not be sent across the wire as strings
                String strVal = IoUtils.readLengthCodedString(in, fieldCount, CHARSET);

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
                    case DECIMAL:
                        value = new BigDecimal(strVal);
                        break;
                    case DATE:
                        value = strVal;
                        break;
                    case TIME:
                        value = strVal;
                        break;
                    case TIMESTAMP:
                        value = strVal;
                        break;
                    case DOUBLE:
                        value = Double.parseDouble(strVal);
                        break;
                    case BLOB:
                        value = strVal;
                        break;
                    default:
                        throw new IllegalStateException("Don't know how to handle column type of "
                                + field.getColumnType());
                }
            }
            values[field.getIndex()] = new DefaultValue(value);
            i++;
            if (i < fields.size()) {
                fieldCount = in.read();
            }
        }
    }
}
