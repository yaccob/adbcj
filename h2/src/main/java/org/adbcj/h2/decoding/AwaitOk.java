package org.adbcj.h2.decoding;

import org.adbcj.h2.H2Connection;
import org.adbcj.h2.H2DbException;
import io.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class AwaitOk extends StatusReadingDecoder {
    public AwaitOk(H2Connection connection) {
        super(connection);
    }

    @Override
    protected ResultAndState processFurther(DataInputStream stream, Channel channel, int status) throws IOException {
        return ResultAndState.newState(new AnswerNextRequest(connection));
    }

    @Override
    protected void requestFailedContinue(H2DbException exception) {
        throw exception;
    }
}
