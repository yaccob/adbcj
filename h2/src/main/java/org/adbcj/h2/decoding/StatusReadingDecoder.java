package org.adbcj.h2.decoding;

import io.netty.channel.Channel;
import org.adbcj.h2.H2Connection;
import org.adbcj.h2.H2DbException;
import org.adbcj.h2.packets.SizeConstants;
import org.adbcj.h2.protocol.StatusCodes;

import java.io.DataInputStream;
import java.io.IOException;


public abstract class StatusReadingDecoder implements DecoderState {
    protected final H2Connection connection;

    protected StatusReadingDecoder(H2Connection connection) {
        this.connection = connection;
    }

    public final ResultAndState decode(DataInputStream stream, Channel channel) throws IOException {
        if(stream.available()< SizeConstants.INT_SIZE){
            return ResultAndState.waitForMoreInput(this);
        }
        final int status = stream.readInt();
        if(StatusCodes.STATUS_ERROR.isStatus(status)){
            ResultOrWait<String> sqlstate = IoUtils.tryReadNextString(stream, ResultOrWait.Start);
            ResultOrWait<String> message = IoUtils.tryReadNextString(stream, sqlstate);
            ResultOrWait<String> sql = IoUtils.tryReadNextString(stream, message);
            ResultOrWait<Integer> errorCode = IoUtils.tryReadNextInt(stream, sql);
            ResultOrWait<String> stackTrace = IoUtils.tryReadNextString(stream, errorCode);
            if(stackTrace.couldReadResult) {
                return handleException(new H2DbException(sqlstate.result,message.result,sql.result,errorCode.result,stackTrace.result));
            }  else{
                return continueWithNextRequest();
            }
        }
        return processFurther(stream, channel, status);
    }

    private ResultAndState continueWithNextRequest() {
        return ResultAndState.waitForMoreInput(this);
    }

    protected abstract ResultAndState processFurther(DataInputStream stream, Channel channel, int status) throws IOException;

    @Override
    public ResultAndState handleException(H2DbException exception) {
        requestFailedContinue(exception);
        return ResultAndState.newState(new AnswerNextRequest(connection));
    }

    protected void requestFailedContinue(H2DbException exception){};


}
