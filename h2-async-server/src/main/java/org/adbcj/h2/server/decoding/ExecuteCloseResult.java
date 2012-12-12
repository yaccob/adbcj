package org.adbcj.h2.server.decoding;

import org.adbcj.h2.decoding.IoUtils;
import org.adbcj.h2.decoding.ResultOrWait;
import org.h2.result.ResultInterface;
import org.h2.util.SmallMap;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class ExecuteCloseResult implements DecoderState {
    private final AcceptCommands acceptCommands;

    public ExecuteCloseResult(AcceptCommands acceptCommands) {
        this.acceptCommands = acceptCommands;
    }

    @Override
    public ResultAndState decode(DataInputStream stream, Channel channel) throws IOException {
        final ResultOrWait<Integer> id = IoUtils.tryReadNextInt(stream, ResultOrWait.Start);
        if(id.couldReadResult){
            final SmallMap cache = acceptCommands.cache();
            final Integer idValue = id.result;
            ResultInterface result = (ResultInterface) cache.getObject(idValue, true);
            if (result != null) {
                result.close();
                cache.freeObject(idValue);
            }
            return ResultAndState.newState(acceptCommands);
        } else{
            return ResultAndState.waitForMoreInput(this);
        }
    }
}
