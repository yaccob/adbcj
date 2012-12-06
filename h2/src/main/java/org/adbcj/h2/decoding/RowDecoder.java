package org.adbcj.h2.decoding;

import org.adbcj.DbException;
import org.adbcj.Field;
import org.adbcj.ResultHandler;
import org.adbcj.Value;
import org.adbcj.h2.DateTimeUtils;
import org.adbcj.h2.H2Connection;
import org.adbcj.h2.H2DbException;
import org.adbcj.support.DefaultDbSessionFuture;
import org.adbcj.support.DefaultValue;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;
import java.math.BigDecimal;
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

    @Override
    public ResultAndState handleException(H2DbException exception) {
        resultFuture.trySetException(exception);
        return ResultAndState.newState(new AnswerNextRequest((H2Connection) resultFuture.getSession()));
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
        int typeCode = maybeType.result;
        H2Types type = H2Types.typeCodeToType(typeCode);
        switch (type) {
            case NULL:
                return convertToValue(ResultOrWait.result(null));
            case LONG:
                return convertToValue(IoUtils.tryReadNextLong(stream, maybeType));
            case DATE:
                return convertToDateValue(IoUtils.tryReadNextLong(stream, maybeType));
            case TIME:
                return convertNanoToTime(IoUtils.tryReadNextLong(stream, maybeType));
            case TIMESTAMP:
                final ResultOrWait<Long> dateValues = IoUtils.tryReadNextLong(stream, maybeType);
                final ResultOrWait<Long> nanos = IoUtils.tryReadNextLong(stream, dateValues);
                return convertToTimestampValue(dateValues, nanos);
            case INTEGER:
                return convertToValue(IoUtils.tryReadNextInt(stream, maybeType));
            case DOUBLE:
                return convertToValue(IoUtils.tryReadNextDouble(stream, maybeType));
            case DECIMAL:
                return convertToDecimalValue(IoUtils.tryReadNextString(stream, maybeType));
            case STRING:
                return convertToValue(IoUtils.tryReadNextString(stream, maybeType));
            case CLOB:
                final ResultOrWait<Long> length = IoUtils.tryReadNextLong(stream, ResultOrWait.Start);
                if(length.couldReadResult){
                    final ResultOrWait<Value> value
                            = convertToValue(IoUtils.readEncodedString(stream, length.result.intValue()));
                    final ResultOrWait<Integer> lobMagicBits = IoUtils.tryReadNextInt(stream, value);
                    if(lobMagicBits.couldReadResult){
                        return value;
                    }else{
                        return ResultOrWait.WaitLonger;
                    }

                } else{
                    return ResultOrWait.WaitLonger;
                }
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

    private ResultOrWait<Value> convertToDecimalValue(ResultOrWait<String> maybeValue) {
        if (!maybeValue.couldReadResult) {
            return ResultOrWait.WaitLonger;
        } else {
            return ResultOrWait.result((Value) new DefaultValue(new BigDecimal(maybeValue.result)));
        }
    }
    private ResultOrWait<Value> convertNanoToTime(ResultOrWait<Long> maybeValue) {
        if (!maybeValue.couldReadResult) {
            return ResultOrWait.WaitLonger;
        } else {
            return ResultOrWait.result((Value) new DefaultValue(DateTimeUtils.convertNanoToTime(maybeValue.result)));
        }
    }
    private ResultOrWait<Value> convertToDateValue(ResultOrWait<Long> maybeValue) {
        if (!maybeValue.couldReadResult) {
            return ResultOrWait.WaitLonger;
        } else {
            return ResultOrWait.result((Value) new DefaultValue(DateTimeUtils.convertDateValueToDate(maybeValue.result)));
        }
    }
    private ResultOrWait<Value> convertToTimestampValue(ResultOrWait<Long> dateValue,ResultOrWait<Long> nanos) {
        if (dateValue.couldReadResult && nanos.couldReadResult) {
            return ResultOrWait.result((Value) new DefaultValue(
                    DateTimeUtils.convertDateValueToTimestamp(dateValue.result,nanos.result)));
        } else {
            return ResultOrWait.WaitLonger;
        }
    }
}
