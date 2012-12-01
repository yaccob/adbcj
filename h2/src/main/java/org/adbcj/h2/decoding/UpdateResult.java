package org.adbcj.h2.decoding;

import org.adbcj.Result;
import org.adbcj.h2.H2DbException;
import org.adbcj.support.DefaultDbSessionFuture;
import org.adbcj.support.DefaultResult;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @author roman.stoffel@gamlor.info
 */
public class UpdateResult  extends StatusReadingDecoder  {
    private final DefaultDbSessionFuture<Result> resultHandler;

    public UpdateResult(DefaultDbSessionFuture<Result> resultHandler) {
        super(connectionOfFuture(resultHandler));
        this.resultHandler = resultHandler;
    }

    @Override
    protected ResultAndState processFurther(DataInputStream stream, Channel channel, int status) throws IOException {
        StatusCodes.STATUS_OK.expectStatusOrThrow(status);

        final ResultOrWait<Integer> affected = IoUtils.tryReadNextInt(stream, ResultOrWait.Start);
        final ResultOrWait<Boolean> autoCommit = IoUtils.tryReadNextBoolean(stream, affected);
        if(autoCommit.couldReadResult){
            resultHandler.setResult(new DefaultResult(affected.result.longValue(),new ArrayList<String>()));
            return ResultAndState.newState(new AnswerNextRequest(connection));
        } else{
            return ResultAndState.waitForMoreInput(this);
        }
    }

    @Override
    protected void handleException(H2DbException exception) {
        resultHandler.trySetException(exception);
    }
}
