package org.adbcj.mysql.codec.packets;

import org.adbcj.mysql.codec.IoUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

/**
 * @author roman.stoffel@gamlor.info
 * @since 11.04.12
 */
public class PreparedStatementRequest extends CommandRequest {
    private final int statementId;
    private final Object[] data;

    public PreparedStatementRequest(int statementId, Object[] data) {
        super(Command.STATEMENT_EXECUTE);
        this.statementId = statementId;
        this.data = data;
    }

    @Override
    public boolean hasPayload() {
        return true;
    }

    /**
     * Protocol see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Execute_Packet_.28Tentative_Description.29
     */
    @Override
    protected void writePayLoad(OutputStream out, String charset) throws IOException {
        IoUtils.writeInt(out, statementId);
        out.write((byte)1); // flags: 1: CURSOR_TYPE_READ_ONLY
        IoUtils.writeInt(out,1); // reserved for future use. Currently always 1.
        out.write(IoUtils.nullMask(data));  //null_bit_map
        out.write(1); //  new_parameter_bound_flag

    }

    @Override
    public int getLength(String charset) throws UnsupportedEncodingException {
        return 1;
    }


}
