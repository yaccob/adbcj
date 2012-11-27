package org.adbcj.h2.decoding;

import org.adbcj.h2.H2Connection;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class AnswerNextRequest extends StatusReadingDecoder {
    private final H2Connection connection;

    public AnswerNextRequest(H2Connection connection) {
        this.connection = connection;
    }

    @Override
    protected ResultAndState processFurther(DataInputStream stream, Channel channel, int status) throws IOException {
        final DecoderState request = connection.dequeRequest();
        stream.reset();
        stream.mark(Integer.MAX_VALUE);
        return request.decode(stream, channel);
    }
}
