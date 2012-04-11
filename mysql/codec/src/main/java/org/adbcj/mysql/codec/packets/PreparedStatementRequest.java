package org.adbcj.mysql.codec.packets;

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
    public int getLength(String charset) throws UnsupportedEncodingException {
        return 1;
    }


}
