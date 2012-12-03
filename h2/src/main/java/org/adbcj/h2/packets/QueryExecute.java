package org.adbcj.h2.packets;

import org.adbcj.h2.decoding.H2Types;
import org.adbcj.h2.decoding.IoUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Types;

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
            // nothing
            return;
        }
        if (Integer.class.isInstance(param)) {
            stream.writeInt(Types.INTEGER);
            stream.writeInt(((Integer) param).intValue());
        } else if (String.class.isInstance(param)) {
            stream.writeInt(H2Types.STRING);
            IoUtils.writeString(stream, param.toString());
        } else {
            throw new Error("TODO: Not implmeneted yet");
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
        }
        if (Integer.class.isInstance(param)) {
            return SizeConstants.INT_SIZE+SizeConstants.INT_SIZE;
        } else if (String.class.isInstance(param)) {
            return SizeConstants.INT_SIZE+SizeConstants.INT_SIZE * SizeConstants.CHAR_SIZE * ((String) param).toCharArray().length;
        } else {
            throw new Error("TODO: Not implmeneted yet");
        }
    }
}
