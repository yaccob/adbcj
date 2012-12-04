package org.adbcj.h2.decoding;

import org.adbcj.h2.H2Connection;
import org.adbcj.h2.H2DbException;
import org.adbcj.support.DefaultDbFuture;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class AnswerNextRequest extends StatusReadingDecoder {

    public AnswerNextRequest(H2Connection connection) {
        super(connection);
    }

    @Override
    protected ResultAndState processFurther(DataInputStream stream, Channel channel, int status) throws IOException {
        return handleRequest(stream, channel);
    }

    @Override
    protected void handleException(H2DbException exception) {
        final DefaultDbFuture<Object> toComplete = connection.dequeRequest().getToComplete();
        toComplete.trySetException(exception);
    }

    private ResultAndState handleRequest(DataInputStream stream, Channel channel) throws IOException {
        final DecoderState request = connection.dequeRequest().getStartState();
        stream.reset();
        stream.mark(Integer.MAX_VALUE);
        return request.decode(stream, channel);
    }
}
