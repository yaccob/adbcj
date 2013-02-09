package org.adbcj.mysql.codec.decoding;

import org.adbcj.ResultHandler;
import org.adbcj.mysql.codec.*;
import org.adbcj.mysql.codec.packets.ResultSetFieldResponse;
import org.adbcj.support.DefaultDbSessionFuture;
import io.netty.channel.Channel;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
* @since 12.04.12
*/
public class FieldDecodingState<T> extends DecoderState {
    private final int expectedAmountOfFields;
    private final List<MysqlField> fields;
    private final DefaultDbSessionFuture<T> future;
    private final ResultHandler<T> eventHandler;
    private final T accumulator;
    private Row.RowDecodingType decodingType;

    public FieldDecodingState(Row.RowDecodingType decodingType,
                              int expectedAmountOfFields,
                              List<MysqlField> fields,
                              DefaultDbSessionFuture<T> future,
                              ResultHandler<T> eventHandler,
                              T accumulator) {
        this.decodingType = decodingType;

        this.expectedAmountOfFields = expectedAmountOfFields;
        this.fields = fields;
        this.future = future;
        this.eventHandler = eventHandler;
        this.accumulator = accumulator;
    }

    @Override
    public ResultAndState parse(int length,
                                int packetNumber,
                                BoundedInputStream in, Channel channel) throws IOException {

        int fieldNo = fields.size();

        if(logger.isTraceEnabled()){
            logger.trace("expectedAmountOfFields: {} current field {}", expectedAmountOfFields,fieldNo);

        }
        ResultSetFieldResponse resultSetFieldResponse = decodeFieldResponse(in, length, packetNumber,fieldNo);


        ArrayList<MysqlField> newFields = new ArrayList<MysqlField>(fieldNo+1);
        newFields.addAll(fields);
        newFields.add(resultSetFieldResponse.getField());
        eventHandler.field(resultSetFieldResponse.getField(), accumulator);


        if (expectedAmountOfFields > (fieldNo+1)) {
            return result(new FieldDecodingState<T>(decodingType, expectedAmountOfFields,newFields,future,eventHandler, accumulator),resultSetFieldResponse);
        } else{
            return result(new FieldEof<T>(decodingType, newFields,future,eventHandler, accumulator),resultSetFieldResponse);
        }
    }


    private ResultSetFieldResponse decodeFieldResponse(InputStream in,
                                                       int packetLength,
                                                       int packetNumber,
                                                       int fieldNo) throws IOException {
        MysqlField field = parseField(in,fieldNo);
        return new ResultSetFieldResponse(packetLength, packetNumber, field);
    }

    public static MysqlField parseField(InputStream in, int fieldNo) throws IOException {
        String catalogName = IoUtils.readLengthCodedString(in, CHARSET);
        String schemaName = IoUtils.readLengthCodedString(in, CHARSET);
        String tableLabel = IoUtils.readLengthCodedString(in, CHARSET);
        String tableName = IoUtils.readLengthCodedString(in, CHARSET);
        String columnLabel = IoUtils.readLengthCodedString(in, CHARSET);
        String columnName = IoUtils.readLengthCodedString(in, CHARSET);
        in.read(); // Skip filler
        int characterSetNumber = IoUtils.readUnsignedShort(in);
        MysqlCharacterSet charSet = MysqlCharacterSet.findById(characterSetNumber);
        long length = IoUtils.readUnsignedInt(in);
        int fieldTypeId = in.read();
        MysqlType fieldType = MysqlType.findById(fieldTypeId);
        Set<FieldFlag> flags = IoUtils.readEnumSet(in, FieldFlag.class);
        int decimals = in.read();
        IoUtils.safeSkip(in, 2); // Skip filler
        long fieldDefault = IoUtils.readBinaryLengthEncoding(in);
        return new MysqlField(fieldNo, catalogName, schemaName, tableLabel, tableName, fieldType, columnLabel,
                columnName, 0, // Figure out precision
                decimals, charSet, length, flags, fieldDefault);
    }

}
