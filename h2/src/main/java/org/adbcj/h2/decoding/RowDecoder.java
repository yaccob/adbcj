package org.adbcj.h2.decoding;

import io.netty.channel.Channel;
import org.adbcj.Field;
import org.adbcj.ResultHandler;
import org.adbcj.Value;
import org.adbcj.h2.H2Connection;
import org.adbcj.h2.H2DbException;
import org.adbcj.h2.protocol.ReadUtils;
import org.adbcj.support.DefaultDbFuture;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

/**
 * @author roman.stoffel@gamlor.info
 */
public class RowDecoder<T> implements DecoderState {
    private final ResultHandler<T> eventHandler;
    private final T accumulator;
    private final DefaultDbFuture<T> resultFuture;
    private final H2Connection connection;
    private final List<Field> fields;
    private final int availableRows;
    private final int rowToRead;

    public RowDecoder(ResultHandler<T> eventHandler,
                      T accumulator,
                      DefaultDbFuture<T> resultFuture,
                      H2Connection connection,
                      List<Field> fields,
                      int availableRows) {
        this(eventHandler, accumulator, resultFuture,connection,fields, availableRows, 0);
    }

    public RowDecoder(ResultHandler<T> eventHandler,
                      T accumulator,
                      DefaultDbFuture<T> resultFuture,
                      H2Connection connection,
                      List<Field> fields,
                      int availableRows,
                      int rowToRead) {
        this.eventHandler = eventHandler;
        this.accumulator = accumulator;
        this.resultFuture = resultFuture;
        this.connection = connection;
        this.fields = fields;
        this.availableRows = availableRows;
        this.rowToRead = rowToRead;
    }

    @Override
    public ResultAndState decode(DataInputStream stream, Channel channel) throws IOException {
        final ResultOrWait<Boolean> row = IoUtils.tryReadNextBoolean(stream, ResultOrWait.Start);
        if(0==rowToRead){
            eventHandler.startResults(accumulator);
        }
        if(row.couldReadResult && !row.result){
            return finishResultRead();
        }
        return decodeRow(stream, row);

    }

    private ResultAndState decodeRow(DataInputStream stream, ResultOrWait row) throws IOException {
        ResultOrWait<Value> lastValue = (ResultOrWait) row;
        ResultOrWait<Value> values[] = new ResultOrWait[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            final ResultOrWait<Integer> type = IoUtils.tryReadNextInt(stream, lastValue);
            lastValue = ReadUtils.tryReadValue(stream, type);
            values[i] = lastValue;
        }
        if (lastValue.couldReadResult) {
            eventHandler.startRow(accumulator);
            for (ResultOrWait<Value> value : values) {
                eventHandler.value(value.result, accumulator);
            }
            eventHandler.endRow(accumulator);

            if((rowToRead+1)==availableRows){
                return finishResultRead();
            } else{
                return ResultAndState.newState(
                        new RowDecoder<T>(eventHandler, accumulator, resultFuture,connection, fields, availableRows,rowToRead+1)
                );
            }
        } else{
            return ResultAndState.waitForMoreInput(this);
        }
    }

    @Override
    public ResultAndState handleException(H2DbException exception) {
        resultFuture.trySetException(exception);
        return ResultAndState.newState(new AnswerNextRequest(connection));
    }

    private ResultAndState finishResultRead() {
        eventHandler.endResults(accumulator);
        resultFuture.trySetResult(accumulator);
        return ResultAndState.newState(new AnswerNextRequest(connection));
    }

}
