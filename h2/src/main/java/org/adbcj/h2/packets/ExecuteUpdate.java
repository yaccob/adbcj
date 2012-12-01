package org.adbcj.h2.packets;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class ExecuteUpdate implements  ClientToServerPacket {
    public static final int COMMAND_EXECUTE_UPDATE = 3;
    private final int id;

    public ExecuteUpdate(int id) {
        this.id = id;
    }

    @Override
    public void writeToStream(DataOutputStream stream) throws IOException {
        stream.writeInt(COMMAND_EXECUTE_UPDATE);
        stream.writeInt(id);
        stream.writeInt(0);
    }

    @Override
    public int getLength() {
        return SizeConstants.INT_SIZE + // command execute update
                SizeConstants.INT_SIZE + // command id
                SizeConstants.INT_SIZE + // parameters amount
                0;
    }
}
