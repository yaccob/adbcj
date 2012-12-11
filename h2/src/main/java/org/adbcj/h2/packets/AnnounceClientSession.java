package org.adbcj.h2.packets;

import org.adbcj.h2.CancellationToken;
import org.adbcj.h2.decoding.IoUtils;
import org.adbcj.h2.protocol.CommandCodes;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class AnnounceClientSession extends ClientToServerPacket {


    private final String sessionId;

    public AnnounceClientSession(String sessionId) {
        super(CancellationToken.NO_CANCELLATION);
        this.sessionId = sessionId;
    }

    @Override
    public void writeToStream(DataOutputStream stream) throws IOException {
        stream.writeInt(CommandCodes.SESSION_SET_ID.getCommandValue());
        IoUtils.writeString(stream, sessionId);
    }

    @Override
    public int getLength() {
        return SizeConstants.INT_SIZE +  // request type
                SizeConstants.INT_SIZE + // length field
                sessionId.toCharArray().length*SizeConstants.CHAR_SIZE + // data of string
                0;
    }
}
