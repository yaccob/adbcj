package org.adbcj.h2.server.responses;

import org.adbcj.h2.packets.SizeConstants;
import org.adbcj.h2.protocol.StatusCodes;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class UndoLogPosResponse implements ServerToClientPacket {
    private final int undoLogPos;

    public UndoLogPosResponse(int undoLogPos) {
        this.undoLogPos = undoLogPos;
    }

    @Override
    public void writeToStream(DataOutputStream stream) throws IOException {
        stream.writeInt(StatusCodes.STATUS_OK.getStatusValue());
        stream.writeInt(undoLogPos);
    }

    @Override
    public int getLength() {
        return SizeConstants.INT_SIZE + // status
                SizeConstants.INT_SIZE + // value for undo log
                0;
    }
}
