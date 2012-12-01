package org.adbcj.h2.decoding;

import org.adbcj.ResultHandler;
import org.adbcj.h2.H2Connection;
import org.adbcj.h2.H2DbException;
import org.adbcj.h2.packets.SizeConstants;
import org.adbcj.support.DefaultDbSessionFuture;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class QueryPrepare<T> extends StatusReadingDecoder {
    private final ResultHandler<T> eventHandler;
    private final T accumulator;
    private final DefaultDbSessionFuture<T> resultFuture;

    public QueryPrepare(ResultHandler<T> eventHandler, T accumulator, DefaultDbSessionFuture<T> resultFuture) {
        super((H2Connection) resultFuture.getSession());
        this.eventHandler = eventHandler;
        this.accumulator = accumulator;
        this.resultFuture = resultFuture;
    }

    @Override
    protected ResultAndState processFurther(DataInputStream stream, Channel channel, int status) throws IOException {
        StatusCodes.STATUS_OK.expectStatusOrThrow(status);
        if(stream.available()>=(SizeConstants.BYTE_SIZE+SizeConstants.BYTE_SIZE+ SizeConstants.INT_SIZE)){
            boolean isQuery = IoUtils.readBoolean(stream);
            boolean readonly = IoUtils.readBoolean(stream);
            int paramsSite = stream.readInt();
            return ResultAndState.newState(new QueryHeader<T>(eventHandler,accumulator,resultFuture));
        } else{
            return ResultAndState.waitForMoreInput(this);
        }
    }

    @Override
    protected void handleException(H2DbException exception) {
        resultFuture.trySetException(exception);
    }
}
