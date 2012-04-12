package org.adbcj.mysql.codec.packets;

import org.adbcj.mysql.codec.MysqlType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author roman.stoffel@gamlor.info
 * @since 12.04.12
 */
public final class StatementPreparedEOF extends ServerPacket {

    private final int handlerId;
    private final int colums;
    private List<MysqlType> parametersTypes;

    public StatementPreparedEOF(int packetLength, int packetNumber, OkResponse.PreparedStatementOK preparedStatement) {
        super(packetLength, packetNumber);
        this.handlerId = preparedStatement.getHandlerId();
        this.colums = preparedStatement.getColumns();
        this.parametersTypes = new ArrayList<MysqlType>();
    }
    public StatementPreparedEOF(int packetLength, int packetNumber, PreparedStatementToBuild preparedStatement) {
        super(packetLength, packetNumber);
        this.handlerId = preparedStatement.getHandlerId();
        this.colums = preparedStatement.getColumns();
        this.parametersTypes = preparedStatement.getParametersTypes();
    }


    public int getHandlerId() {
        return handlerId;
    }

    public int getColumns() {
        return colums;
    }

    public List<MysqlType> getParametersTypes() {
        return parametersTypes;
    }
}
