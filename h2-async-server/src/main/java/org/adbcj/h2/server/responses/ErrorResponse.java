package org.adbcj.h2.server.responses;

import org.adbcj.h2.decoding.IoUtils;
import org.adbcj.h2.protocol.StatusCodes;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.DbException;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;

import static org.adbcj.h2.packets.SizeConstants.sizeOf;

/**
 * @author roman.stoffel@gamlor.info
 */
public class ErrorResponse implements ServerToClientPacket{

    private final String trace;
    private final String message;
    private final String sql;
    private final SQLException e;

    public ErrorResponse(DbException ex) {
        this.e = DbException.convert(ex).getSQLException();
        StringWriter writer = new StringWriter();
        e.printStackTrace(new PrintWriter(writer));
        this.trace = writer.toString();
        if (e instanceof JdbcSQLException) {
            JdbcSQLException j = (JdbcSQLException) e;
            this.message = j.getOriginalMessage();
            this.sql = j.getSQL();
        } else {
            this.message = e.getMessage();
            this.sql = null;
        }
    }

    @Override
    public void writeToStream(DataOutputStream stream) throws IOException {
        stream.writeInt(StatusCodes.STATUS_ERROR.getStatusValue());
        IoUtils.writeString(stream, e.getSQLState());
        IoUtils.writeString(stream, message);
        IoUtils.writeString(stream, sql);
        stream.writeInt(e.getErrorCode());
        IoUtils.writeString(stream, trace);
    }

    @Override
    public int getLength() {
        return sizeOf(StatusCodes.STATUS_ERROR.getStatusValue()) +
               sizeOf(e.getSQLState())+
               sizeOf(message)+
               sizeOf(sql)+
               sizeOf(e.getErrorCode())+
               sizeOf(trace)+
                0;
    }
}
