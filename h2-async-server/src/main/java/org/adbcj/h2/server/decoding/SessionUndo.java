package org.adbcj.h2.server.decoding;

import org.adbcj.h2.server.responses.UndoLogPosResponse;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class SessionUndo implements DecoderState {
    private final AcceptCommands acceptCommands;

    public SessionUndo(AcceptCommands acceptCommands) {
        this.acceptCommands = acceptCommands;
    }

    @Override
    public ResultAndState decode(DataInputStream stream, Channel channel) throws IOException {
        final int undoLogPos = acceptCommands.session().getUndoLogPos();
        channel.write(new UndoLogPosResponse(undoLogPos));
        return ResultAndState.newState(acceptCommands);
    }
}
