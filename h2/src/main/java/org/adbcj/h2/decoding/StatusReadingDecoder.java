package org.adbcj.h2.decoding;

import org.adbcj.h2.H2Connection;
import org.adbcj.h2.H2DbException;
import org.adbcj.h2.packets.SizeConstants;
import org.adbcj.support.DefaultDbSessionFuture;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
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
                handleException(new H2DbException(sqlstate.result,message.result,sql.result,errorCode.result,stackTrace.result));
                return ResultAndState.newState(new AnswerNextRequest(connection));
            }  else{
                return ResultAndState.waitForMoreInput(this);
            }
        }
        return processFurther(stream, channel, status);
    }
    protected abstract ResultAndState processFurther(DataInputStream stream, Channel channel, int status) throws IOException;
    protected abstract void handleException(H2DbException exception);


    public static H2Connection connectionOfFuture(DefaultDbSessionFuture<?> future){
        return (H2Connection) future.getSession();
    }
}
