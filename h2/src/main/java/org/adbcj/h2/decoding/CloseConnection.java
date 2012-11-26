package org.adbcj.h2.decoding;

import org.adbcj.support.DefaultDbFuture;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class CloseConnection extends StatusReadingDecoder {
    private final DefaultDbFuture<Void> finishedClose;

    public CloseConnection(DefaultDbFuture<Void> finishedClose) {
        this.finishedClose = finishedClose;
    }

    @Override
    protected ResultAndState processFurther(DataInputStream stream, Channel channel, int status) throws IOException {
        StatusCodes.STATUS_OK.expectStatusOrThrow(status);
        channel.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                finishedClose.setResult(null);
            }
        });
        return ResultAndState.newState(ClosedConnectionState.INSTANCE);
    }
}
