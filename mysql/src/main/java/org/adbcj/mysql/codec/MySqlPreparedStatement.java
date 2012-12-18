package org.adbcj.mysql.codec;

import org.adbcj.*;
import org.adbcj.mysql.codec.packets.StatementPreparedEOF;

/**
 * @author roman.stoffel@gamlor.info
 * @since 11.04.12
 */
public class MySqlPreparedStatement implements PreparedQuery, PreparedUpdate {
    private final MySqlConnection connection;
    private final StatementPreparedEOF statementInfo;
    private volatile boolean isOpen = true;

    public MySqlPreparedStatement(MySqlConnection connection,
                                  StatementPreparedEOF statementInfo) {
        this.connection = connection;
        this.statementInfo = statementInfo;
    }

    private void validateParameters(Object[] params) {
        if(isClosed()){
            throw new IllegalStateException("Cannot execute closed statement");
        }
        if (params.length != statementInfo.getParametersTypes().size()) {
            throw new IllegalArgumentException("Expect " + statementInfo.getParametersTypes().size() + " paramenters " +
                    "but got " + params.length + " parameters");
        }
    }

    @Override
    public boolean isClosed() {
        return connection.isClosed() || !isOpen;
    }

    @Override
    public DbSessionFuture execute(Object... params) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public <T> DbSessionFuture<T> executeWithCallback(ResultHandler<T> eventHandler, T accumulator, Object... params) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public DbFuture<Void> close() {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }
}
