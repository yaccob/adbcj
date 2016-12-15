package org.adbcj.h2.decoding;

import io.netty.channel.Channel;
import org.adbcj.Connection;
import org.adbcj.PreparedQuery;
import org.adbcj.h2.H2Connection;
import org.adbcj.h2.H2DbException;
import org.adbcj.h2.H2PreparedQuery;
import org.adbcj.h2.packets.SizeConstants;
import org.adbcj.h2.protocol.StatusCodes;
import org.adbcj.support.DefaultDbFuture;

import java.io.DataInputStream;
import java.io.IOException;


public abstract class StatementPrepare<T> extends StatusReadingDecoder {
    private final DefaultDbFuture<T> resultFuture;

    public StatementPrepare(DefaultDbFuture<T> resultFuture, H2Connection connection) {
        super(connection);
        this.resultFuture = resultFuture;
    }

    @Override
    protected ResultAndState processFurther(DataInputStream stream, Channel channel, int status) throws IOException {
        StatusCodes.STATUS_OK.expectStatusOrThrow(status);
        if(stream.available()>=(SizeConstants.BYTE_SIZE+SizeConstants.BYTE_SIZE+ SizeConstants.INT_SIZE)){
            boolean isQuery = IoUtils.readBoolean(stream);
            boolean readonly = IoUtils.readBoolean(stream);
            int paramsCount = stream.readInt();
            handleCompletion(connection,paramsCount);
            return ResultAndState.newState(new AnswerNextRequest(connection));
        } else{
            return ResultAndState.waitForMoreInput(this);
        }
    }

    protected abstract void handleCompletion(H2Connection connection, int paramsCount);


    @Override
    protected void requestFailedContinue(H2DbException exception) {
        resultFuture.trySetException(exception);
    }

    public static StatementPrepare<Connection> createOnlyPassFailure(
            final DefaultDbFuture<Connection> resultFuture,
            final H2Connection connection) {
        return new StatementPrepare<Connection>(resultFuture,connection ) {
            @Override
            protected void handleCompletion(H2Connection connection, int paramsCount) {
            }
        };
    }
    public static StatementPrepare<Connection> completeFuture(
            final DefaultDbFuture<Connection> resultFuture,
            final H2Connection connection) {
        return new StatementPrepare<Connection>(resultFuture,connection ) {
            @Override
            protected void handleCompletion(H2Connection connection, int paramsCount) {
                resultFuture.trySetResult(connection);
            }
        };
    }
}
