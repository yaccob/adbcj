package org.adbcj.h2.decoding;

import org.adbcj.Connection;
import org.adbcj.DbException;
import org.adbcj.h2.H2Connection;
import org.adbcj.h2.H2DbException;
import org.adbcj.h2.packets.AnnounceClientSession;
import org.adbcj.h2.packets.SizeConstants;
import org.adbcj.support.DefaultDbFuture;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
class FirstServerHandshake extends StatusReadingDecoder {
    private final DefaultDbFuture<Connection> currentState;
    private final H2Connection connection;

    public FirstServerHandshake(DefaultDbFuture<Connection> currentState, H2Connection connection) {
        super(connection);
        this.currentState = currentState;
        this.connection = connection;
    }


    protected ResultAndState processFurther(DataInputStream input,
                                            Channel channel,
                                            int status) throws IOException {
        StatusCodes.STATUS_OK.expectStatusOrThrow(status);
        if (input.available() < SizeConstants.INT_SIZE) {
            return ResultAndState.waitForMoreInput(this);
        }
        int clientVersion = input.readInt();
        if (clientVersion != Constants.TCP_PROTOCOL_VERSION_12) {
            throw new DbException("This version only supports version " + Constants.TCP_PROTOCOL_VERSION_12
                    + ", but server wants " + clientVersion);
        }
        channel.write(new AnnounceClientSession(connection.getSessionId()));
        return ResultAndState.newState(new SessionIdReceived(currentState, connection));
    }

    @Override
    protected void handleException(H2DbException exception) {
        currentState.trySetException(exception);
    }
}

class SessionIdReceived extends StatusReadingDecoder {
    private final DefaultDbFuture<Connection> currentState;
    private final H2Connection connection;

    SessionIdReceived(DefaultDbFuture<Connection> currentState, H2Connection connection) {
        super(connection);
        this.currentState = currentState;
        this.connection = connection;
    }

    @Override
    protected ResultAndState processFurther(DataInputStream input, Channel channel, int status) throws IOException {
        StatusCodes.STATUS_OK.expectStatusOrThrow(status);
        currentState.setResult(connection);
        return ResultAndState.newState(new AnswerNextRequest(connection));
    }

    @Override
    protected void handleException(H2DbException exception) {
        this.currentState.trySetException(exception);
    }
}
