package org.adbcj.h2.server.decoding;

import org.adbcj.h2.decoding.IoUtils;
import org.adbcj.h2.decoding.ResultOrWait;
import org.adbcj.h2.server.responses.UpdateResponse;
import org.h2.command.Command;
import org.h2.engine.SessionRemote;
import org.h2.expression.Parameter;
import org.h2.expression.ParameterInterface;
import org.h2.value.Value;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author roman.stoffel@gamlor.info
 */
class ExecuteUpdate implements DecoderState {
    private static final Logger log = LoggerFactory.getLogger(ExecuteQuery.class);
    private final AcceptCommands acceptCommands;

    public ExecuteUpdate(AcceptCommands acceptCommands) {
        this.acceptCommands = acceptCommands;
    }

    @Override
    public ResultAndState decode(DataInputStream stream, Channel channel) throws IOException {
        final ResultOrWait<Integer> id = IoUtils.tryReadNextInt(stream, ResultOrWait.Start);
        final ResultOrWait<Integer> paramsLength = IoUtils.tryReadNextInt(stream, id);
        final ResultOrWait<List<Value>> paramsData = ReadUtils.tryReadParams(stream, paramsLength);

        if (paramsData.couldReadResult) {
            return executeUpdate(channel, id, paramsData);

        } else {
            return ResultAndState.waitForMoreInput(this);
        }


    }

    private ResultAndState executeUpdate(Channel channel, ResultOrWait<Integer> id, ResultOrWait<List<Value>> paramsData) {
        Command command = (Command) acceptCommands.cache().getObject(id.result, false);

        if(log.isDebugEnabled()){
            log.debug("Executing query statement: {} with id: {}",command,id.result);
        }
        bindParameters(paramsData, command);
        int old = acceptCommands.session().getModificationId();
        int updateCount;
        synchronized (acceptCommands.session()) {
            updateCount = command.executeUpdate();
        }
        int status;
        if (acceptCommands.session().isClosed()) {
            status = SessionRemote.STATUS_CLOSED;
        } else {
            status = acceptCommands.getState(old);
        }
        final boolean autoCommit = acceptCommands.session().getAutoCommit();
        channel.write(new UpdateResponse(status,updateCount,autoCommit));
        return ResultAndState.newState(acceptCommands);
    }

    static void bindParameters(ResultOrWait<List<Value>> paramsData, Command command) {

        final ArrayList<? extends ParameterInterface> params = command.getParameters();
        if(params.size()!=paramsData.result.size()){
            throw new IllegalStateException("Expect parameter count "+params.size()+
                " but got "+paramsData.result.size());
        }
        for (int i = 0; i < params.size(); i++) {
            ((Parameter)params.get(i)).setValue(paramsData.result.get(i));
        }
    }
}
