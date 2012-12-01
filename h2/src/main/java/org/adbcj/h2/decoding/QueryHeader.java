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
class QueryHeader<T> extends StatusReadingDecoder {
    private final ResultHandler<T> eventHandler;
    private final T accumulator;
    private final DefaultDbSessionFuture<T> resultFuture;

    public QueryHeader(ResultHandler<T> eventHandler,
                       T accumulator,
                       DefaultDbSessionFuture<T> resultFuture) {
        super((H2Connection) resultFuture.getSession());
        this.eventHandler = eventHandler;
        this.accumulator = accumulator;
        this.resultFuture = resultFuture;
    }

    @Override
    protected ResultAndState processFurther(final DataInputStream stream,
                                            Channel channel,
                                            int status) throws IOException {
        StatusCodes.STATUS_OK.expectStatusOrThrow(status);

        if(stream.available()< (SizeConstants.INT_SIZE +SizeConstants.INT_SIZE)){
            return ResultAndState.waitForMoreInput(this);
        }  else{
            int columnCount = stream.readInt();
            int rowCount = stream.readInt();
            eventHandler.startFields(accumulator);
            return ResultAndState.newState(
                    new ColumnDecoder<T>(eventHandler,
                            accumulator,
                            resultFuture, rowCount, columnCount)
            );
        }

    }

    @Override
    protected void handleException(H2DbException exception) {
        this.resultFuture.setException(exception);
    }
}

