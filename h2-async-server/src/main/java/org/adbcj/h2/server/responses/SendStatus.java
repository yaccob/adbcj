package org.adbcj.h2.server.responses;

import org.adbcj.h2.packets.SizeConstants;
import org.adbcj.h2.protocol.StatusCodes;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class SendStatus implements ServerToClientPacket {
    private final StatusCodes status;

    public SendStatus(StatusCodes status) {
        this.status = status;
    }

    @Override
    public void writeToStream(DataOutputStream stream) throws IOException {
        stream.writeInt(status.getStatusValue());
    }

    @Override
    public int getLength() {
        return SizeConstants.sizeOf(status.getStatusValue());

    }
}
