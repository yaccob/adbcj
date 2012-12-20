package org.adbcj.mysql.codec;

import org.adbcj.*;
import org.adbcj.mysql.codec.packets.StatementPreparedEOF;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.DefaultResultEventsHandler;
import org.adbcj.support.DefaultResultSet;

/**
 * @author roman.stoffel@gamlor.info
 * @since 11.04.12
 */
public class MySqlPreparedStatement implements PreparedQuery, PreparedUpdate {
    private final MySqlConnection connection;
    private final StatementPreparedEOF statementInfo;
    private volatile DefaultDbFuture<Void> closeFuture = null;

    public MySqlPreparedStatement(MySqlConnection connection,
                                  StatementPreparedEOF statementInfo) {
        this.connection = connection;
        this.statementInfo = statementInfo;
    }

    protected void validateParameters(Object[] params) {
        if(isClosed()){
            throw new IllegalStateException("Cannot execute closed statement");
        }
        if (params.length != statementInfo.getParametersTypes().size()) {
            throw new IllegalArgumentException("Expect " + statementInfo.getParametersTypes().size() + " paramenters " +
                    "but got " + params.length + " parameters");
        }
    }


    @Override
    public DbSessionFuture execute(Object... params) {
        return (DbSessionFuture)executeWithCallback(new DefaultResultEventsHandler(),new DefaultResultSet(),params);
    }

    @Override
    public <T> DbSessionFuture<T> executeWithCallback(ResultHandler<T> eventHandler, T accumulator, Object... params) {
        validateParameters(params);
        return (DbSessionFuture<T>) connection.queRequest(
                MySqlRequests.executePreparedQuery(
                        statementInfo, params, eventHandler, accumulator, connection
                )).getFuture();
    }

    @Override
    public boolean isClosed() {
        return closeFuture!=null || connection.isClosed();
    }


    @Override
    public DbFuture<Void> close() {
        synchronized (connection.lock()){
            if(closeFuture==null){
                closeFuture = (DefaultDbFuture<Void>) connection.queRequest(
                        MySqlRequests.closeStatemeent(
                                statementInfo, connection
                        )).getFuture();
            }
            return closeFuture;
        }
    }
}
