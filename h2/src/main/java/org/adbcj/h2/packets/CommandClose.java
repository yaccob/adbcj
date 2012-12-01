package org.adbcj.h2.packets;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class CommandClose implements ClientToServerPacket {
    public static final int COMMAND_CLOSE = 4;
    private int id;

    public CommandClose(int id) {
        this.id = id;
    }

    @Override
    public void writeToStream(DataOutputStream stream) throws IOException {
        stream.writeInt(COMMAND_CLOSE);
        stream.writeInt(id);
    }

    @Override
    public int getLength() {
        return SizeConstants.INT_SIZE + // command close
                SizeConstants.INT_SIZE + // command id
                0;
    }
}
