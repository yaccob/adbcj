package org.adbcj.h2.server.decoding;


import org.adbcj.h2.decoding.IoUtils;
import org.adbcj.h2.decoding.ResultOrWait;
import org.adbcj.h2.server.responses.PrepareResponse;
import org.h2.command.Command;
import org.h2.expression.ParameterInterface;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @author roman.stoffel@gamlor.info
 */
class PreparedStatement implements DecoderState {
    private final AcceptCommands commandsAccepting;
    private final boolean requestedParameters;

    public PreparedStatement(AcceptCommands commandsAccepting,
                             boolean requestedParameters) {

        this.commandsAccepting = commandsAccepting;
        this.requestedParameters = requestedParameters;
    }

    @Override
    public ResultAndState decode(DataInputStream stream,
                                 Channel channel) throws IOException {
        final ResultOrWait<Integer> id = IoUtils.tryReadNextInt(stream, ResultOrWait.Start);
        final ResultOrWait<String> sql = IoUtils.tryReadNextString(stream, id);
        if(sql.couldReadResult){
            int old = commandsAccepting.session().getModificationId();
            Command command = commandsAccepting.session().prepareLocal(sql.result);


            boolean readonly = command.isReadOnly();
            commandsAccepting.cache().addObject(id.result, command);
            boolean isQuery = command.isQuery();
            ArrayList<? extends ParameterInterface> params = command.getParameters();

            channel.write(new PrepareResponse(commandsAccepting.getState(old),
                    readonly,
                    isQuery,
                    params,requestedParameters));

            return ResultAndState.newState(commandsAccepting);
        } else{
            return ResultAndState.waitForMoreInput(this);
        }
    }
}
