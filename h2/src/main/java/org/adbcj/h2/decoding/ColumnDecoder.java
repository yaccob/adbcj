package org.adbcj.h2.decoding;

import org.adbcj.ResultHandler;
import org.adbcj.support.DefaultDbSessionFuture;
import org.adbcj.support.DefaultField;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

import static org.adbcj.h2.decoding.IoUtils.*;

/**
 * @author roman.stoffel@gamlor.info
 */
public class ColumnDecoder<T>  implements DecoderState {
    private final ResultHandler<T> eventHandler;
    private final T accumulator;
    private final DefaultDbSessionFuture<T> resultFuture;
    private final int rows;
    private final int columnsAvailable;
    private final int columnToParse;

    public ColumnDecoder(ResultHandler<T> eventHandler,
                         T accumulator,
                         DefaultDbSessionFuture<T> resultFuture,
                         int rows,
                         int columnsAvailable,
                         int columnToParse) {
        this.eventHandler = eventHandler;
        this.accumulator = accumulator;
        this.resultFuture = resultFuture;
        this.rows = rows;
        this.columnsAvailable = columnsAvailable;
        this.columnToParse = columnToParse;
    }

    @Override
    public ResultAndState decode(DataInputStream stream, Channel channel) throws IOException {
        ResultOrWait<String> alias = tryReadNextString(stream, ResultOrWait.Start);
        ResultOrWait<String> schemaName = tryReadNextString(stream, alias);
        ResultOrWait<String> tableName = tryReadNextString(stream, schemaName);
        ResultOrWait<String> columnName = tryReadNextString(stream, tableName);
        ResultOrWait<Integer> columnType = tryReadNextInt(stream, columnName);
        ResultOrWait<Long> precision = tryReadNextLong(stream, columnType);
        ResultOrWait<Integer> scale = tryReadNextInt(stream, precision);
        ResultOrWait<Integer> displaySize = tryReadNextInt(stream, scale);
        ResultOrWait<Boolean> autoIncrement = tryReadNextBoolean(stream, displaySize);
        ResultOrWait<Integer> nullable = tryReadNextInt(stream, autoIncrement);

        if(nullable.couldReadResult){
            final DefaultField field = new DefaultField(columnToParse,
                    "",
                    schemaName.result,
                    tableName.result,
                    columnName.result,
                    H2Types.typeCodeToType(columnType.result),
                    columnName.result,
                    columnName.result,
                    (int) precision.result.intValue(),
                    scale.result,
                    autoIncrement.result,
                    false,
                    false,
                    1 == nullable.result,
                    true, true, "");

            eventHandler.field(field,accumulator );
            if((columnToParse+1)==columnsAvailable){
                eventHandler.endFields(accumulator);
                return ResultAndState.waitForMoreInput(
                        new RowDecoder<T>(eventHandler,
                                accumulator,
                                resultFuture,rows)
                );
            } else{
                return ResultAndState.newState(
                        new ColumnDecoder<T>(eventHandler,
                                accumulator,
                                resultFuture,rows, columnsAvailable, columnToParse+1));
            }
        } else{
            return ResultAndState.waitForMoreInput(this);
        }
    }
}
