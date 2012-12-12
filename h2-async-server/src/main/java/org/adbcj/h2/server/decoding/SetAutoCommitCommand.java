package org.adbcj.h2.server.decoding;

import org.adbcj.h2.decoding.IoUtils;
import org.adbcj.h2.decoding.ResultOrWait;
import org.adbcj.h2.protocol.StatusCodes;
import org.adbcj.h2.server.responses.SendStatus;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class SetAutoCommitCommand  implements DecoderState {
    private final AcceptCommands acceptCommands;

    public SetAutoCommitCommand(AcceptCommands acceptCommands) {
        this.acceptCommands = acceptCommands;
    }

    @Override
    public ResultAndState decode(DataInputStream stream, Channel channel) throws IOException {
        final ResultOrWait<Boolean> stateToSet = IoUtils.tryReadNextBoolean(stream, ResultOrWait.Start);
        if(stateToSet.couldReadResult){
            acceptCommands.session().setAutoCommit(stateToSet.result);
            channel.write(new SendStatus(StatusCodes.STATUS_OK));
            return ResultAndState.newState(acceptCommands);
        } else{
            return ResultAndState.waitForMoreInput(this);
        }
    }
}
