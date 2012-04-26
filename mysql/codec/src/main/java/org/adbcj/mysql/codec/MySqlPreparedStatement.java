package org.adbcj.mysql.codec;

import org.adbcj.*;
import org.adbcj.mysql.codec.packets.ClosePreparedStatementRequest;
import org.adbcj.mysql.codec.packets.PreparedStatementRequest;
import org.adbcj.mysql.codec.packets.StatementPreparedEOF;
import org.adbcj.support.AbstractDbSession;
import org.adbcj.support.DefaultResultSet;
import org.adbcj.support.ExpectResultRequest;

/**
 * @author roman.stoffel@gamlor.info
 * @since 11.04.12
 */
public class MySqlPreparedStatement implements PreparedStatement {
    private final AbstractMySqlConnection connection;
    private final StatementPreparedEOF statementInfo;
    private volatile boolean isOpen = true;

    public MySqlPreparedStatement(AbstractMySqlConnection connection,
                                  StatementPreparedEOF statementInfo) {
        this.connection = connection;
        this.statementInfo = statementInfo;
    }

    @Override
    public DbFuture<ResultSet> executeQuery(final Object... params) {
        if(isClosed()){
            throw new IllegalStateException("Cannot execute closed statement");
        }
        if (params.length != statementInfo.getParametersTypes().size()) {
            throw new IllegalArgumentException("Expect " + statementInfo.getParametersTypes().size() + " paramenters " +
                    "but got " + params.length + " parameters");
        }
        ResultEventHandler<DefaultResultSet> eventHandler = new AbstractDbSession.DefaultResultEventsHandler();
        DefaultResultSet resultSet = new DefaultResultSet(connection);


        return connection.enqueueTransactionalRequest(new ExecutePrepareStatement(eventHandler, resultSet, params));
    }

    @Override
    public boolean isClosed() {
        return connection.isClosed() || !isOpen;
    }

    @Override
    public DbFuture<Void> close() {
        isOpen  = false;
        DbSessionFuture<Void> future = connection.enqueueTransactionalRequest(new AbstractDbSession.Request<Void>(connection) {
            @Override
            protected void execute() throws Exception {
                ClosePreparedStatementRequest request = new ClosePreparedStatementRequest(statementInfo.getHandlerId());
                connection.write(request);
                complete(null);
            }

        });
        return future;
    }

    public class ExecutePrepareStatement extends ExpectResultRequest {
        private final Object[] params;

        public ExecutePrepareStatement(ResultEventHandler<DefaultResultSet> eventHandler, DefaultResultSet resultSet, Object... params) {
            super(MySqlPreparedStatement.this.connection, eventHandler, resultSet);
            this.params = params;
        }

        @Override
        public void execute() throws Exception {
            PreparedStatementRequest request = new PreparedStatementRequest(statementInfo.getHandlerId(),
                    statementInfo.getParametersTypes(), params);
            connection.write(request);
        }

        @Override
        public String toString() {
            return "Prepared statement execute";
        }
    }
}
