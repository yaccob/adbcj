package org.adbcj.mysql.codec.decoding;

import io.netty.channel.Channel;
import org.adbcj.Field;
import org.adbcj.ResultHandler;
import org.adbcj.Value;
import org.adbcj.mysql.codec.BoundedInputStream;
import org.adbcj.mysql.codec.IoUtils;
import org.adbcj.mysql.codec.MySqlConnection;
import org.adbcj.mysql.codec.MysqlField;
import org.adbcj.mysql.codec.packets.EofResponse;
import org.adbcj.mysql.codec.packets.ResultSetRowResponse;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.DefaultValue;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

/**
 * @since 12.04.12
 */
public class Row<T> extends DecoderState {
    private final RowDecodingType rowDecoding;
    private final List<MysqlField> fields;
    private final DefaultDbFuture<T> future;
    private final MySqlConnection connection;
    private final ResultHandler<T> eventHandler;
    private final T accumulator;

    public Row(RowDecodingType rowDecoding,
               List<MysqlField> fields,
               DefaultDbFuture<T> future,
               MySqlConnection connection,
               ResultHandler<T> eventHandler,
               T accumulator) {
        this.rowDecoding = rowDecoding;
        this.fields = fields;
        this.future = future;
        this.connection = connection;
        this.eventHandler = eventHandler;
        this.accumulator = accumulator;
    }

    @Override
    public ResultAndState parse(int length, int packetNumber,
                                BoundedInputStream in, Channel channel) throws IOException {
        int fieldCount = in.read(); // This is only for checking for EOF
        if (fieldCount == RESPONSE_EOF) {
            eventHandler.endResults(accumulator);
            future.trySetResult(accumulator);
            EofResponse rowEof = decodeEofResponse(in, length, packetNumber, EofResponse.Type.ROW);
            return result(new AcceptNextResponse(connection), rowEof);
        }

        Value[] values = rowDecoding.decode(in, fieldCount, this);
        eventHandler.startRow(accumulator);
        for (Value value : values) {
            eventHandler.value(value, accumulator);
        }
        eventHandler.endRow(accumulator);
        return result(new Row<T>(rowDecoding, fields, future,connection, eventHandler, accumulator), new ResultSetRowResponse(length, packetNumber, values));

    }

    public enum RowDecodingType {
        BINARY {
            @Override
            public <T> Value[] decode(BoundedInputStream in, int fieldCount, Row<T> row) throws IOException {
                Value[] values = new Value[row.fields.size()];
                // 0 (packet header)   should have been read by the calling method
                byte[] nullBits = new byte[(values.length + 7 + 2) / 8];
                in.read(nullBits);
                for (MysqlField field : row.fields) {
                    Object value = null;
                    if (hasValue(field.getIndex(), nullBits)) {
                        switch (field.getMysqlType()) {
                            case LONG:
                                value = IoUtils.readInt(in);
                                break;
                            case LONGLONG:
                                value = IoUtils.readLong(in);
                                break;
                            case VAR_STRING:
                                value = IoUtils.readLengthCodedString(in, in.read(), CHARSET);
                                break;
                            case NEWDECIMAL:
                                value = IoUtils.readLengthCodedString(in, in.read(), CHARSET);
                                break;
                            case DATE:
                                value = IoUtils.readDate(in);
                                break;
                            case DATETIME:
                                value = IoUtils.readDate(in);
                                break;
                            case TIME:
                                value = IoUtils.readDate(in);
                                break;
                            case TIMESTAMP:
                                value = IoUtils.readDate(in);
                                break;
                            case DOUBLE:
                                value = Double.longBitsToDouble(IoUtils.readLong(in));
                                break;
                            case BLOB:
                                value = IoUtils.readLengthCodedString(in, in.read(), CHARSET);
                                break;
                            case NULL:
                                value = null;
                                break;
                            default:
                                throw new IllegalStateException("Not yet implemented for type " + field.getMysqlType());
                        }

                    }

                    values[field.getIndex()] = new DefaultValue(value);
                }

                return values;
            }


            private boolean hasValue(int valuePos, byte[] nullBitMap) {
                int bit = 4; // first two bits are reserved for future use
                int nullMaskPos = 0;
                boolean hasValue = false;
                for (int i = 0; i <= valuePos; i++) {
                    if ((nullBitMap[nullMaskPos] & bit) > 0) {
                        hasValue = false;
                    } else {
                        hasValue = true;
                    }
                    if (((bit <<= 1) & 255) == 0) {
                        bit = 1;
                        nullMaskPos++;
                    }
                }
                return hasValue;
            }
        },
        STRING_BASED {
            @Override
            public <T> Value[] decode(BoundedInputStream in, int fieldCount, Row<T> row) throws IOException {
                Value[] values = new Value[row.fields.size()];
                int i = 0;
                for (Field field : row.fields) {
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
                    if (i < row.fields.size()) {
                        fieldCount = in.read();
                    }
                }
                return values;
            }
        };


        public abstract <T> Value[] decode(BoundedInputStream in, int fieldCount, Row<T> row) throws IOException;
    }

}
