package org.adbcj.mysql.codec.decoding;

import org.adbcj.ResultHandler;
import org.adbcj.mysql.codec.BoundedInputStream;
import org.adbcj.mysql.codec.MysqlField;
import org.adbcj.mysql.codec.packets.EofResponse;
import org.adbcj.support.DefaultDbSessionFuture;
import org.jboss.netty.channel.Channel;

import java.io.IOException;
import java.util.List;

/**
* @since 12.04.12
*/
class FieldEof<T> extends DecoderState {

    private final List<MysqlField> fields;
    private final DefaultDbSessionFuture<T> future;
    private final ResultHandler<T> eventHandler;
    private final T accumulator;
    private Row.RowDecodingType decodingType;

    public FieldEof(Row.RowDecodingType decodingType, List<MysqlField> fields, DefaultDbSessionFuture<T> future, ResultHandler<T> eventHandler, T accumulator) {
        this.decodingType = decodingType;
        //To change body of created methods use File | Settings | File Templates.
        this.fields = fields;
        this.future = future;
        this.eventHandler = eventHandler;
        this.accumulator = accumulator;
    }

    @Override
    public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, Channel channel) throws IOException {
        int fieldCount = in.read();

        eventHandler.endFields(accumulator);
        eventHandler.startResults(accumulator);

        if (fieldCount != RESPONSE_EOF) {
            throw new IllegalStateException("Expected an EOF response from the server");
        }
        EofResponse fieldEof = decodeEofResponse(in, length, packetNumber, EofResponse.Type.FIELD);
        return result(new Row<T>(decodingType, fields,future,eventHandler,accumulator),fieldEof);
    }

    @Override
    public String toString() {
        return "FIELD-EOF";
    }


}
