package org.adbcj.h2.packets;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class CloseCommand implements ClientToServerPacket {

    public static final int SESSION_CLOSE = 1;
    @Override
    public void writeToStream(DataOutputStream stream) throws IOException {
        stream.writeInt(SESSION_CLOSE);
    }

    @Override
    public int getLength() {
        return SizeConstants.INT_SIZE;
    }
}
