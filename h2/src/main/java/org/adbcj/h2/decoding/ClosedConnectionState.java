package org.adbcj.h2.decoding;

import org.adbcj.support.DefaultDbFuture;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class ClosedConnectionState extends StatusReadingDecoder {

    private final DefaultDbFuture<Void> finishedClose;

    public ClosedConnectionState(DefaultDbFuture<Void> finishedClose) {

        this.finishedClose = finishedClose;
    }

    @Override
    protected ResultAndState processFurther(DataInputStream stream, Channel channel, int status) throws IOException {
        finishedClose.trySetResult(null);
        return ResultAndState.waitForMoreInput(this);
    }

}
