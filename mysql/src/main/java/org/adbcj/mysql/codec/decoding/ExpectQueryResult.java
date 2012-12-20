package org.adbcj.mysql.codec.decoding;

import org.adbcj.ResultHandler;
import org.adbcj.mysql.codec.*;
import org.adbcj.mysql.codec.packets.ErrorResponse;
import org.adbcj.mysql.codec.packets.OkResponse;
import org.adbcj.support.DefaultDbSessionFuture;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author roman.stoffel@gamlor.info
 */
public class ExpectQueryResult<T> extends ResponseStart {
    private final ResultHandler<T> eventHandler;
    private final T accumulator;
    protected final DefaultDbSessionFuture<T> future;
    private Row.RowDecodingType decodingType;

    public ExpectQueryResult(Row.RowDecodingType decodingType, DefaultDbSessionFuture<T> future, ResultHandler<T> eventHandler, T accumulator) {
        super((MySqlConnection) future.getSession());
        this.decodingType = decodingType;
        this.future = future;
        this.eventHandler = eventHandler;
        this.accumulator = accumulator;
    }

    @Override
    protected ResultAndState handleError(ErrorResponse errorResponse) {
        future.trySetException(errorResponse.toException());
        return new ResultAndState(new AcceptNextResponse(connection),errorResponse );
    }

    @Override
    protected ResultAndState handleOk(OkResponse.RegularOK regularOK) {
        throw new Error("Not supported for query results");
    }

    @Override
    protected ResultAndState parseAsResult(int length, int packetNumber, BoundedInputStream in, int fieldCount) throws IOException {
        // Get the number of fields. The largest this can be is a 24-bit
        // integer so cast to int is ok
        int expectedFieldPackets = (int) IoUtils.readBinaryLengthEncoding(in, fieldCount);
        logger.trace("Field count {}", expectedFieldPackets);

        Long extra = null;
        if (in.getRemaining() > 0) {
            extra = IoUtils.readBinaryLengthEncoding(in);
        }
        eventHandler.startFields(accumulator);
        return result(new FieldDecodingState(decodingType, expectedFieldPackets,new ArrayList<MysqlField>(),future, eventHandler,accumulator),
                new ResultSetResponse(length, packetNumber, expectedFieldPackets, extra));
    }
}
