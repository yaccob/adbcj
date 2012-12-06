package org.adbcj.h2.decoding;

import org.adbcj.h2.H2DbException;
import org.adbcj.support.DefaultDbSessionFuture;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class CompleteTransaction extends StatusReadingDecoder {
    final DefaultDbSessionFuture<Void> toComplete;

    public CompleteTransaction(DefaultDbSessionFuture<Void> toComplete) {
        super(connectionOfFuture(toComplete));
        this.toComplete = toComplete;
    }

    @Override
    protected ResultAndState processFurther(DataInputStream stream, Channel channel, int status) throws IOException {
        StatusCodes.STATUS_OK.expectStatusOrThrow(status);

        final ResultOrWait<Integer> affected = IoUtils.tryReadNextInt(stream, ResultOrWait.Start);
        final ResultOrWait<Boolean> autoCommit = IoUtils.tryReadNextBoolean(stream, affected);
        final ResultOrWait<Integer> okStatus = IoUtils.tryReadNextInt(stream, autoCommit);
        if (okStatus.couldReadResult) {
            StatusCodes.STATUS_OK.expectStatusOrThrow(okStatus.result);
            toComplete.trySetResult(null);
            return ResultAndState.newState(new AnswerNextRequest(connection));
        } else {
            return ResultAndState.waitForMoreInput(this);
        }
    }

    @Override
    protected void requestFailedContinue(H2DbException exception) {
        throw exception;
    }
}
