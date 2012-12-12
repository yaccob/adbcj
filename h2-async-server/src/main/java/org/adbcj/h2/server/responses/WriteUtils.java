package org.adbcj.h2.server.responses;

import org.adbcj.h2.decoding.IoUtils;
import org.adbcj.h2.packets.SizeConstants;
import org.h2.store.Data;
import org.h2.value.*;

import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;

/**
 * @author roman.stoffel@gamlor.info
 */
class WriteUtils {
    private static final int LOB_MAGIC = 0x1234;


    public static void writeValue(DataOutputStream stream, Value value) throws IOException {
        stream.writeInt(value.getType());
        switch (value.getType()) {
            case Value.NULL:
                return;
            case Value.INT:
                stream.writeInt(value.getInt());
                return;
            case Value.LONG:
                stream.writeLong(value.getLong());
                return;
            case Value.DOUBLE:
                stream.writeDouble(value.getDouble());
                return;
            case Value.STRING:
                IoUtils.writeString(stream, value.getString());
                return;
            case Value.DECIMAL:
                IoUtils.writeString(stream, value.getString());
                return;
            case Value.DATE:
                stream.writeLong(((ValueDate) value).getDateValue());
                return;
            case Value.TIME:
                stream.writeLong(((ValueTime) value).getNanos());
                return;
            case Value.TIMESTAMP:
                ValueTimestamp ts = (ValueTimestamp) value;
                stream.writeLong(ts.getDateValue());
                stream.writeLong(ts.getNanos());
                return;
            case Value.CLOB:
                if (value instanceof ValueLobDb) {
                    ValueLobDb lob = (ValueLobDb) value;
                    if (!lob.isStored()) {
                        long length = value.getPrecision();
                        stream.writeLong(length);
                        Data.copyString(value.getReader(), stream);
                        stream.writeInt(LOB_MAGIC);
                        return;
                    } else {
                        throw new IllegalStateException("Cannot write this type yet: " + value);
                    }

                } else {
                    throw new IllegalStateException("Cannot write this type yet: " + value);
                }
            default:
                throw new IllegalStateException("Cannot write this type yet: " + value);
        }
    }

    public static int calculateParameterSize(Value[] values) {
        int size = SizeConstants.INT_SIZE; // parameter count
        for (Value value : values) {
            size += sizeOf(value);
        }
        return size;
    }

    public static int sizeOf(Value value) {
        return SizeConstants.sizeOf(value.getType()) + sizeOfContent(value);
    }

    static int sizeOfContent(Value value) {
        switch (value.getType()) {
            case Value.NULL:
                return 0;
            case Value.INT:
                return SizeConstants.INT_SIZE;
            case Value.LONG:
                return SizeConstants.LONG_SIZE;
            case Value.DOUBLE:
                return SizeConstants.DOUBLE_SIZE;
            case Value.STRING:
                return SizeConstants.sizeOf(value.getString());
            case Value.DECIMAL:
                return SizeConstants.sizeOf(value.getString());
            case Value.DATE:
                return SizeConstants.LONG_SIZE;
            case Value.TIME:
                return SizeConstants.LONG_SIZE;
            case Value.TIMESTAMP:
                ValueTimestamp ts = (ValueTimestamp) value;
                return SizeConstants.sizeOf(ts.getDateValue()) + SizeConstants.sizeOf(ts.getNanos());
            case Value.CLOB:
                return SizeConstants.LONG_SIZE
                        + value.getSmall().length+
                        SizeConstants.INT_SIZE;
            default:
                throw new IllegalStateException("Cannot write this type yet: " + value);
        }
    }


    private static String toString(BigDecimal decimal) {
        if (null == decimal) {
            return null;
        }
        String p = decimal.toPlainString();
        if (p.length() < 40) {
            return p;
        } else {
            return decimal.toString();
        }
    }
}
