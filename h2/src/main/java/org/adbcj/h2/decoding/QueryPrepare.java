package org.adbcj.h2.decoding;

import org.adbcj.DbException;
import org.adbcj.PreparedQuery;
import org.adbcj.h2.H2Connection;
import org.adbcj.h2.H2DbException;
import org.adbcj.h2.H2PreparedQuery;
import org.adbcj.h2.Request;
import org.adbcj.h2.packets.SizeConstants;
import org.adbcj.support.DefaultDbSessionFuture;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public abstract class QueryPrepare<T> extends StatusReadingDecoder {
    private final DefaultDbSessionFuture<T> resultFuture;
    private final int sessionId;

    public QueryPrepare(DefaultDbSessionFuture<T> resultFuture,
                        int sessionId) {
        super((H2Connection) resultFuture.getSession());
        this.sessionId = sessionId;
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

    public static <T> QueryPrepare<T> continueWithRequest(final Request followUpRequest,
                                                          DefaultDbSessionFuture<T> resultFuture,
                                                          int sessionId){
        return new QueryPrepare<T>(resultFuture, sessionId) {
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

    public static QueryPrepare<PreparedQuery> createPrepareQuery(final DefaultDbSessionFuture<PreparedQuery> resultFuture,
                                                                 final int sessionId) {
        return new QueryPrepare<PreparedQuery>(resultFuture, sessionId) {
            @Override
            protected void handleCompletion(H2Connection connection, int paramsCount) {
                H2PreparedQuery query = new H2PreparedQuery(connection,sessionId,paramsCount);
                resultFuture.trySetResult(query);
            }
        };
    }
}
