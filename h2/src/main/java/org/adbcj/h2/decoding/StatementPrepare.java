package org.adbcj.h2.decoding;

import org.adbcj.Connection;
import org.adbcj.DbException;
import org.adbcj.PreparedQuery;
import org.adbcj.PreparedUpdate;
import org.adbcj.h2.*;
import org.adbcj.h2.packets.SizeConstants;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.DefaultDbSessionFuture;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public abstract class StatementPrepare<T> extends StatusReadingDecoder {
    private final DefaultDbFuture<T> resultFuture;

    public StatementPrepare(DefaultDbSessionFuture<T> resultFuture) {
        super((H2Connection) resultFuture.getSession());
        this.resultFuture = resultFuture;
    }
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
    protected void handleException(H2DbException exception) {
        resultFuture.trySetException(exception);
    }

    public static <T> StatementPrepare<T> continueWithRequest(final Request followUpRequest,
                                                          DefaultDbSessionFuture<T> resultFuture){
        return new StatementPrepare<T>(resultFuture) {
            @Override
            protected void handleCompletion(H2Connection connection, int paramsCount) {
                if(paramsCount==0){
                    connection.queResponseHandlerAndSendMessage(followUpRequest);
                }else{
                    throw new DbException("Implementation error: Expect 0 parameters, but got: "+paramsCount);
                }
            }
        };
    }

    public static StatementPrepare<PreparedQuery> createPrepareQuery(final DefaultDbSessionFuture<PreparedQuery> resultFuture,
                                                                     final int sessionId) {
        return new StatementPrepare<PreparedQuery>(resultFuture) {
            @Override
            protected void handleCompletion(H2Connection connection, int paramsCount) {
                H2PreparedQuery query = new H2PreparedQuery(connection,sessionId,paramsCount);
                resultFuture.trySetResult(query);
            }
        };
    }

    public static StatementPrepare<PreparedUpdate> createPrepareUpdate(final DefaultDbSessionFuture<PreparedUpdate> resultFuture,
                                                                     final int sessionId) {
        return new StatementPrepare<PreparedUpdate>(resultFuture) {
            @Override
            protected void handleCompletion(H2Connection connection, int paramsCount) {
                H2PreparedUpdate query = new H2PreparedUpdate(connection,sessionId,paramsCount);
                resultFuture.trySetResult(query);
            }
        };
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
