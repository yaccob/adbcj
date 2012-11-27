package org.adbcj.h2.packets;

import org.adbcj.h2.decoding.IoUtils;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class DirectQuery implements ClientToServerPacket{
    public static final int SESSION_PREPARE = 0;
    public static final int COMMAND_EXECUTE_QUERY = 2;
    public static final int RESULT_CLOSE = 7;
    public static final int COMMAND_CLOSE = 4;
    private int id;
    private int queryId;
    private String sql;

    public DirectQuery(int id, int queryId, String sql) {
        this.id = id;
        this.queryId = queryId;
        this.sql = sql;
    }

    @Override
    public void writeToStream(DataOutputStream stream) throws IOException {
        stream.writeInt(SESSION_PREPARE);
        stream.writeInt(id);
        IoUtils.writeString(stream, sql);
        stream.writeInt(COMMAND_EXECUTE_QUERY);
        stream.writeInt(id);
        stream.writeInt(queryId);
        stream.writeInt(Integer.MAX_VALUE); // max rows size
        stream.writeInt(Integer.MAX_VALUE); // fetch size
        stream.writeInt(0); // parameters
        stream.writeInt(RESULT_CLOSE);
        stream.writeInt(queryId);
        stream.writeInt(COMMAND_CLOSE);
        stream.writeInt(id);
    }

    @Override
    public int getLength() {
        return SizeConstants.INT_SIZE +  // Command type
                SizeConstants.INT_SIZE + // Command id
                SizeConstants.INT_SIZE +sql.toCharArray().length * SizeConstants.CHAR_SIZE + // SQL statement
                SizeConstants.INT_SIZE + // Query command
                SizeConstants.INT_SIZE + // again, command id
                SizeConstants.INT_SIZE + // query id
                SizeConstants.INT_SIZE + // max rows size
                SizeConstants.INT_SIZE + // fetch size
                SizeConstants.INT_SIZE + // parameters
                SizeConstants.INT_SIZE + // result close
                SizeConstants.INT_SIZE + // result id
                SizeConstants.INT_SIZE + // command close
                SizeConstants.INT_SIZE + // command id
                0;
    }
}
