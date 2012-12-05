package org.adbcj.h2.packets;

import org.adbcj.h2.CancellationToken;
import org.adbcj.support.DefaultDbFuture;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class CommandClose extends ClientToServerPacket {
    public static final int COMMAND_CLOSE = 4;
    private int id;
    // Some close requests have no response, so we close as soon as we've sent the command
    private final DefaultDbFuture<Void> optionalCloseOnSent;

    public CommandClose(int id) {
        super(CancellationToken.NO_CANCELLATION);
        this.id = id;
        this.optionalCloseOnSent = null;
    }
    public CommandClose(int id, DefaultDbFuture<Void> optionalCloseOnSent) {
        super(CancellationToken.NO_CANCELLATION);
        this.id = id;
        this.optionalCloseOnSent = optionalCloseOnSent;
    }

    @Override
    public void writeToStream(DataOutputStream stream) throws IOException {
        stream.writeInt(COMMAND_CLOSE);
        stream.writeInt(id);
        if(null!=optionalCloseOnSent){
            optionalCloseOnSent.trySetResult(null);
        }
    }

    @Override
    public int getLength() {
        return SizeConstants.INT_SIZE + // command close
                SizeConstants.INT_SIZE + // command id
                0;
    }
}
