package org.adbcj.mysql.codec.packets;

/**
 * @author roman.stoffel@gamlor.info
 * @since 12.04.12
 */
public class StatementPreparedEOF extends ServerPacket {
    private final OkResponse.PreparedStatementOK preparedStatement;

    public StatementPreparedEOF(int packetLength, int packetNumber, OkResponse.PreparedStatementOK preparedStatement) {
        super(packetLength, packetNumber);
        this.preparedStatement = preparedStatement;
    }

    public int getWarnings() {
        return preparedStatement.getWarnings();
    }

    public int getHandlerId() {
        return preparedStatement.getHandlerId();
    }

    public int getColumns() {
        return preparedStatement.getColumns();
    }

    public int getParams() {
        return preparedStatement.getParams();
    }

    public int getFiller() {
        return preparedStatement.getFiller();
    }
}
