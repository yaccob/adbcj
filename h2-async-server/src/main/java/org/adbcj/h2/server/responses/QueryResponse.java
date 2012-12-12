package org.adbcj.h2.server.responses;

import org.adbcj.h2.decoding.IoUtils;
import org.adbcj.h2.packets.SizeConstants;
import org.h2.result.ResultInterface;
import org.h2.value.Value;

import java.io.DataOutputStream;
import java.io.IOException;

import static org.adbcj.h2.packets.SizeConstants.sizeOf;

/**
 * @author roman.stoffel@gamlor.info
 */
public class QueryResponse implements ServerToClientPacket {
    private final int state;
    private final int columnCount;
    private final int rowCount;
    private final ResultInterface result;
    private final int fetch;

    public QueryResponse(int state, int columnCount, int rowCount, ResultInterface result, int fetch) {
        this.state = state;
        this.columnCount = columnCount;
        this.rowCount = rowCount;
        this.result = result;
        this.fetch = fetch;
    }

    @Override
    public void writeToStream(DataOutputStream stream) throws IOException {
        stream.writeInt(state);
        stream.writeInt(columnCount);
        stream.writeInt(rowCount);
        writeColoumns(stream);
        writeRows(stream);
    }

    private void writeColoumns(DataOutputStream stream) throws IOException {
        for (int i = 0; i < columnCount; i++) {
            IoUtils.writeString(stream, result.getAlias(i));
            IoUtils.writeString(stream, result.getSchemaName(i));
            IoUtils.writeString(stream, result.getTableName(i));
            IoUtils.writeString(stream, result.getColumnName(i));
            stream.writeInt(result.getColumnType(i));
            stream.writeLong(result.getColumnPrecision(i));
            stream.writeInt(result.getColumnScale(i));
            stream.writeInt(result.getDisplaySize(i));
            stream.writeBoolean(result.isAutoIncrement(i));
            stream.writeInt(result.getNullable(i));
        }
    }

    private void writeRows(DataOutputStream stream) throws IOException {
        for (int i = 0; i < fetch; i++) {
            writeRow(stream);
        }
    }

    private void writeRow(DataOutputStream stream) throws IOException {
        if (result.next()) {
            stream.writeBoolean(true);
            Value[] v = result.currentRow();
            for (int i = 0; i < result.getVisibleColumnCount(); i++) {
                WriteUtils.writeValue(stream, v[i]);
            }
        } else {
            stream.writeBoolean(false);
        }
    }

    @Override
    public int getLength() {
        return sizeOf(state) + // status
                sizeOf(columnCount) + // column count
                sizeOf(rowCount) + // rowCount count
                sizeOfColums() +
                sizeOfRows() +
                0;
    }

    private int sizeOfColums() {
        int size = 0;
        for (int i = 0; i < columnCount; i++) {
            size += sizeOf(result.getAlias(i));
            size += sizeOf(result.getSchemaName(i));
            size += sizeOf(result.getTableName(i));
            size += sizeOf(result.getColumnName(i));
            size += sizeOf(result.getColumnType(i));
            size += sizeOf(result.getColumnPrecision(i));
            size += sizeOf(result.getColumnScale(i));
            size += sizeOf(result.getDisplaySize(i));
            size += sizeOf(result.isAutoIncrement(i));
            size += sizeOf(result.getNullable(i));
        }
        return size;
    }

    private int sizeOfRows() {
        int size = 0;
        for (int i = 0; i < fetch; i++) {
            size += sizeOfRow();
        }
        result.reset();
        return size;
    }

    private int sizeOfRow() {
        int size = 0;
        if (result.next()) {
            size += SizeConstants.sizeOf(true);
            Value[] v = result.currentRow();
            for (int i = 0; i < result.getVisibleColumnCount(); i++) {
                size +=WriteUtils.sizeOf(v[i]);
            }
        } else {
            size += SizeConstants.sizeOf(false);
        }
        return size;
    }
}
