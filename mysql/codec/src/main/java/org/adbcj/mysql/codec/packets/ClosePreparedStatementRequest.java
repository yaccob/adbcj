package org.adbcj.mysql.codec.packets;

import org.adbcj.mysql.codec.IoUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

/**
 * @author roman.stoffel@gamlor.info
 * @since 26.04.12
 */
public class ClosePreparedStatementRequest extends CommandRequest {
    private final int statementId;

    public ClosePreparedStatementRequest(int statementId) {
        super(Command.STATEMENT_CLOSE);
        this.statementId = statementId;
    }


    @Override
    public boolean hasPayload() {
        return true;
    }
    @Override
    public int getLength(String charset) throws UnsupportedEncodingException {
        return 1+4;
    }
    @Override
    protected void writePayLoad(OutputStream out, String charset) throws IOException {
        IoUtils.writeInt(out, statementId);
    }
}
