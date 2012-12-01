package org.adbcj.h2.decoding;

import org.adbcj.DbException;
import org.adbcj.h2.H2Connection;
import org.adbcj.h2.H2DbException;
import org.adbcj.h2.Request;
import org.adbcj.h2.packets.SizeConstants;
import org.adbcj.support.DefaultDbSessionFuture;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class QueryPrepare<T> extends StatusReadingDecoder {
    private final DefaultDbSessionFuture<T> resultFuture;
    private final Request followUpRequest;
    private final int sessionId;

    public QueryPrepare(Request followUpRequest,
                        DefaultDbSessionFuture<T> resultFuture,
                        int sessionId) {
        super((H2Connection) resultFuture.getSession());
        this.followUpRequest = followUpRequest;
        this.sessionId = sessionId;
        this.resultFuture = resultFuture;
    }

    @Override
    protected ResultAndState processFurther(DataInputStream stream, Channel channel, int status) throws IOException {
        StatusCodes.STATUS_OK.expectStatusOrThrow(status);
        if(stream.available()>=(SizeConstants.BYTE_SIZE+SizeConstants.BYTE_SIZE+ SizeConstants.INT_SIZE)){
            boolean isQuery = IoUtils.readBoolean(stream);
            boolean readonly = IoUtils.readBoolean(stream);
            int paramsSite = stream.readInt();
            if(0!=paramsSite){
                throw new DbException("TODO");
            } else{
                connection.queResponseHandlerAndSendMessage(followUpRequest);

            }
            return ResultAndState.newState(new AnswerNextRequest(connection));
        } else{
            return ResultAndState.waitForMoreInput(this);
        }
    }


    @Override
    protected void handleException(H2DbException exception) {
        resultFuture.trySetException(exception);
    }
}
