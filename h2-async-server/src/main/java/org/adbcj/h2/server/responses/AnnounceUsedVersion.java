package org.adbcj.h2.server.responses;

import org.adbcj.h2.protocol.StatusCodes;
import org.adbcj.h2.packets.SizeConstants;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class AnnounceUsedVersion implements ServerToClientPacket{
    private final int version;

    public AnnounceUsedVersion(int version) {
        this.version = version;
    }

    @Override
    public void writeToStream(DataOutputStream stream) throws IOException {
        stream.writeInt(StatusCodes.STATUS_OK.getStatusValue());
        stream.writeInt(version);
    }

    @Override
    public int getLength() {
        return SizeConstants.INT_SIZE + SizeConstants.INT_SIZE;
    }
}
