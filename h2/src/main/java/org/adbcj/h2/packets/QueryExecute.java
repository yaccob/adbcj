package org.adbcj.h2.packets;

import org.adbcj.h2.DateTimeUtils;
import org.adbcj.h2.decoding.H2Types;
import org.adbcj.h2.decoding.IoUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;

/**
 * @author roman.stoffel@gamlor.info
 */
public class QueryExecute implements ClientToServerPacket {
    public static final int COMMAND_EXECUTE_QUERY = 2;
    public static final int RESULT_CLOSE = 7;
    private int id;
    private int queryId;
    private final Object[] params;

    public QueryExecute(int id, int queryId, Object[] params) {
        this.id = id;
        this.queryId = queryId;
        this.params = params;
    }

    @Override
    public void writeToStream(DataOutputStream stream) throws IOException {
        stream.writeInt(COMMAND_EXECUTE_QUERY);
        stream.writeInt(id);
        stream.writeInt(queryId);
        stream.writeInt(Integer.MAX_VALUE); // max rows size
        stream.writeInt(Integer.MAX_VALUE); // fetch size
        writeParams(stream, params);
        stream.writeInt(RESULT_CLOSE);
        stream.writeInt(queryId);
    }

    @Override
    public int getLength() {
        return SizeConstants.INT_SIZE + // Query command
                SizeConstants.INT_SIZE + // command id
                SizeConstants.INT_SIZE + // query id
                SizeConstants.INT_SIZE + // max rows size
                SizeConstants.INT_SIZE + // fetch size
                calculateParameterSize(params) +
                SizeConstants.INT_SIZE + // result close
                SizeConstants.INT_SIZE + // result id
                0;
    }

    private void writeParams(DataOutputStream stream, Object[] params) throws IOException {
        stream.writeInt(params.length); // parameters
        for (Object param : params) {
            writeValue(stream, param);
        }
    }

    private void writeValue(DataOutputStream stream, Object param) throws IOException {
        if (param == null) {
            stream.writeInt(H2Types.NULL.id());
        } else if (Integer.class.isInstance(param)) {
            stream.writeInt(H2Types.INTEGER.id());
            stream.writeInt(((Integer) param).intValue());
        } else if (Long.class.isInstance(param)) {
            stream.writeInt(H2Types.LONG.id());
            stream.writeLong(((Long) param).longValue());
        } else if (Double.class.isInstance(param)) {
            stream.writeInt(H2Types.LONG.id());
            stream.writeDouble(((Double) param).longValue());
        } else if (Date.class.isInstance(param)) {
            stream.writeInt(H2Types.DATE.id());
            stream.writeLong(DateTimeUtils.dateValueFromDate(((Date)param).getTime()));
        } else if (String.class.isInstance(param)) {
            stream.writeInt(H2Types.STRING.id());
            IoUtils.writeString(stream, param.toString());
        }else if (BigDecimal.class.isInstance(param)) {
            stream.writeInt(H2Types.DECIMAL.id());
            IoUtils.writeString(stream, toString((BigDecimal)param));
        } else {
            throw new Error("TODO: Not implemented yet");
        }

    }

    private int calculateParameterSize(Object[] params) {
        int size = SizeConstants.INT_SIZE; // parameter count
        for (Object param : params) {
            size += sizeOf(param);
        }
        return size;
    }

    private int sizeOf(Object param) {
        if (param == null) {
            return SizeConstants.INT_SIZE;
        } else if (Integer.class.isInstance(param)) {
            return SizeConstants.INT_SIZE+SizeConstants.INT_SIZE;
        } else if (Long.class.isInstance(param)) {
            return SizeConstants.INT_SIZE+SizeConstants.LONG_SIZE;
        }else if (Double.class.isInstance(param)) {
            return SizeConstants.INT_SIZE+SizeConstants.DOUBLE_SIZE;
        }else if (Date.class.isInstance(param)) {
            return SizeConstants.INT_SIZE+SizeConstants.LONG_SIZE;
        }else if (BigDecimal.class.isInstance(param)) {
            return SizeConstants.INT_SIZE+SizeConstants.lengthOfString(toString((BigDecimal) param));
        } else if (String.class.isInstance(param)) {
            return SizeConstants.INT_SIZE+ SizeConstants.INT_SIZE * SizeConstants.lengthOfString((String) param);
        } else {
            throw new Error("Serializing this type "+param.getClass()+" is not supported yet");
        }
    }


    private String toString(BigDecimal decimal){
        if(null==decimal){
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
