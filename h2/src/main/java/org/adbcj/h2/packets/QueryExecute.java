package org.adbcj.h2.packets;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class QueryExecute  implements ClientToServerPacket{
    public static final int COMMAND_EXECUTE_QUERY = 2;
    public static final int RESULT_CLOSE = 7;
    private int id;
    private int queryId;

    public QueryExecute(int id, int queryId) {
        this.id = id;
        this.queryId = queryId;
    }

    @Override
    public void writeToStream(DataOutputStream stream) throws IOException {
        stream.writeInt(COMMAND_EXECUTE_QUERY);
        stream.writeInt(id);
        stream.writeInt(queryId);
        stream.writeInt(Integer.MAX_VALUE); // max rows size
        stream.writeInt(Integer.MAX_VALUE); // fetch size
        stream.writeInt(0); // parameters
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
                SizeConstants.INT_SIZE + // parameters
                SizeConstants.INT_SIZE + // result close
                SizeConstants.INT_SIZE + // result id
                0;
    }
}
