package org.adbcj.h2.decoding;

import org.adbcj.h2.H2Connection;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class AnswerNextRequest extends DecoderState {
    private final H2Connection connection;

    public AnswerNextRequest(H2Connection connection) {
        this.connection = connection;
    }

    @Override
    protected ResultAndState processFurther(DataInputStream stream, Channel channel, int status) throws IOException {
        return ResultAndState.switchState(connection.dequeRequest());
    }
}
