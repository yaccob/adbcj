package org.adbcj.h2.decoding;

import org.adbcj.h2.H2Connection;
import org.adbcj.h2.H2DbException;
import org.adbcj.h2.Request;
import io.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;


public final class AnswerNextRequest extends StatusReadingDecoder {

    public AnswerNextRequest(H2Connection connection) {
        super(connection);
    }

    @Override
    protected ResultAndState processFurther(DataInputStream stream, Channel channel, int status) throws IOException {
        return handleRequest(stream, channel);
    }

    @Override
    public ResultAndState handleException(H2DbException exception) {
        final Request requestInfo = connection.dequeRequest();
        final DecoderState requestDecoder = requestInfo.getStartState();
        return requestDecoder.handleException(exception);
    }

    private ResultAndState handleRequest(DataInputStream stream, Channel channel) throws IOException {
        final Request requestInfo = connection.dequeRequest();
        final DecoderState requestDecoder = requestInfo.getStartState();
        stream.reset();
        stream.mark(Integer.MAX_VALUE);
        return requestDecoder.decode(stream, channel);
    }
}
