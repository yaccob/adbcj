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
class ChangeSessionId implements DecoderState {
    private final AcceptCommands commandsAccepting;

    public ChangeSessionId(AcceptCommands commandsAccepting) {

        this.commandsAccepting = commandsAccepting;
    }

    @Override
    public ResultAndState decode(DataInputStream stream,
                                 Channel channel) throws IOException {
        final ResultOrWait<String> sessionParse = IoUtils.tryReadNextString(stream, ResultOrWait.Start);
        if(sessionParse.couldReadResult){
            String sessionId = sessionParse.result;
            channel.write(new SendStatus(StatusCodes.STATUS_OK));
            return ResultAndState.newState(commandsAccepting);

        } else{
            return ResultAndState.waitForMoreInput(this);
        }
    }

}
