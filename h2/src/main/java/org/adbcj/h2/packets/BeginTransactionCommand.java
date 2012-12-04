package org.adbcj.h2.packets;

import org.adbcj.h2.decoding.IoUtils;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class BeginTransactionCommand implements ClientToServerPacket {
    public static final int SESSION_SET_AUTOCOMMIT = 15;
    @Override
    public void writeToStream(DataOutputStream stream) throws IOException {
        stream.writeInt(SESSION_SET_AUTOCOMMIT);
        IoUtils.writeBoolean(stream, false);
    }

    @Override
    public int getLength() {
        return SizeConstants.INT_SIZE + SizeConstants.BYTE_SIZE;
    }
}
