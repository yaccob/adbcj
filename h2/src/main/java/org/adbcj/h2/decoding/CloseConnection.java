package org.adbcj.h2.decoding;

import org.adbcj.h2.H2Connection;
import org.adbcj.h2.H2DbException;
import org.adbcj.h2.protocol.StatusCodes;
import org.adbcj.support.DefaultDbFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class CloseConnection extends StatusReadingDecoder {
    private final DefaultDbFuture<Void> finishedClose;

    public CloseConnection(DefaultDbFuture<Void> finishedClose, H2Connection connection) {
        super(connection);
        this.finishedClose = finishedClose;
    }

    @Override
    protected ResultAndState processFurther(DataInputStream stream, Channel channel, int status) throws IOException {
        StatusCodes.STATUS_OK.expectStatusOrThrow(status);
        channel.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                finishedClose.trySetResult(null);
            }
        });
        return ResultAndState.newState(new ClosedConnectionState(finishedClose,connection));
    }

    @Override
    protected void requestFailedContinue(H2DbException exception) {
        finishedClose.trySetException(exception);
    }
}
