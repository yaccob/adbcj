package org.adbcj.h2.decoding;

import org.adbcj.DbException;
import org.adbcj.Field;
import org.adbcj.ResultHandler;
import org.adbcj.Value;
import org.adbcj.h2.H2Connection;
import org.adbcj.support.DefaultDbSessionFuture;
import org.adbcj.support.DefaultValue;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;
import java.sql.Types;
import java.util.List;

/**
 * @author roman.stoffel@gamlor.info
 */
public class RowDecoder<T> implements DecoderState {
    private final ResultHandler<T> eventHandler;
    private final T accumulator;
    private final DefaultDbSessionFuture<T> resultFuture;
    private final List<Field> fields;
    private final int availableRows;
    private final int rowToRead;

    public RowDecoder(ResultHandler<T> eventHandler,
                      T accumulator,
                      DefaultDbSessionFuture<T> resultFuture,
                      List<Field> fields,
                      int availableRows) {
        this(eventHandler, accumulator, resultFuture,fields, availableRows, 0);
    }

    public RowDecoder(ResultHandler<T> eventHandler,
                      T accumulator,
                      DefaultDbSessionFuture<T> resultFuture,
                      List<Field> fields,
                      int availableRows,
                      int rowToRead) {
        this.eventHandler = eventHandler;
        this.accumulator = accumulator;
        this.resultFuture = resultFuture;
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
            lastValue = tryReadValue(stream, type);
            values[i] = lastValue;
        }
        if (lastValue.couldReadResult) {
            if (rowToRead == 0) {
                eventHandler.startResults(accumulator);
            }
            eventHandler.startRow(accumulator);
            for (ResultOrWait<Value> value : values) {
                eventHandler.value(value.result, accumulator);
            }
            eventHandler.endRow(accumulator);

            if((rowToRead+1)==availableRows){
                return finishResultRead();
            } else{
                return ResultAndState.newState(
                        new RowDecoder<T>(eventHandler, accumulator, resultFuture, fields, availableRows,rowToRead+1)
                );
            }
        } else{
            return ResultAndState.waitForMoreInput(this);
        }
    }

    private ResultAndState finishResultRead() {
        eventHandler.endResults(accumulator);
        resultFuture.trySetResult(accumulator);
        return ResultAndState.newState(new AnswerNextRequest((H2Connection) resultFuture.getSession()));
    }

    private ResultOrWait<Value> tryReadValue(DataInputStream stream, ResultOrWait<Integer> maybeType) throws IOException {
        if (!maybeType.couldReadResult) {
            return ResultOrWait.WaitLonger;
        }
        int type = maybeType.result;
        switch (type) {
            case Types.NULL:
                return convertToValue(ResultOrWait.result(null));
            case Types.INTEGER:
                return convertToValue(IoUtils.tryReadNextInt(stream, maybeType));
            case H2Types.STRING:
                return convertToValue(IoUtils.tryReadNextString(stream, maybeType));
            default:
                throw new DbException("Cannot handle type: " + type);
        }
    }

    private <T> ResultOrWait<Value> convertToValue(ResultOrWait<T> maybeValue) {
        if (!maybeValue.couldReadResult) {
            return ResultOrWait.WaitLonger;
        } else {
            return ResultOrWait.result((Value) new DefaultValue(maybeValue.result));
        }

    }
}
