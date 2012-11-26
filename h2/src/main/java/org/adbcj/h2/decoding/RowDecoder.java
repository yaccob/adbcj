package org.adbcj.h2.decoding;

import org.adbcj.ResultHandler;
import org.adbcj.support.DefaultDbSessionFuture;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class RowDecoder<T> implements DecoderState  {
    private final ResultHandler<T> eventHandler;
    private final T accumulator;
    private final DefaultDbSessionFuture<T> resultFuture;
    private final int availableRows;

    public RowDecoder(ResultHandler<T> eventHandler,
                      T accumulator,
                      DefaultDbSessionFuture<T> resultFuture,
                      int availableRows) {
        this.eventHandler = eventHandler;
        this.accumulator = accumulator;
        this.resultFuture = resultFuture;
        this.availableRows = availableRows;
    }

    @Override
    public ResultAndState decode(DataInputStream stream, Channel channel) throws IOException {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }
}
