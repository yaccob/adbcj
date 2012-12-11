package org.adbcj.h2.server.responses;

import org.adbcj.h2.packets.SizeConstants;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class UpdateResponse implements ServerToClientPacket{
    private final int status;
    private final int updateCount;
    private final boolean autoCommit;

    public UpdateResponse(int status, int updateCount, boolean autoCommit) {
        this.status = status;
        this.updateCount = updateCount;
        this.autoCommit = autoCommit;
    }

    @Override
    public void writeToStream(DataOutputStream stream) throws IOException {
        stream.writeInt(status);
        stream.writeInt(updateCount);
        stream.writeBoolean(autoCommit);
    }

    @Override
    public int getLength() {
        return SizeConstants.INT_SIZE +  // status
               SizeConstants.INT_SIZE +  // update count
               SizeConstants.BOOLEAN_SIZE +  // autocommit
               0;

    }
}
