package org.adbcj.mysql.codec;

import org.adbcj.DbFuture;
import org.adbcj.PreparedStatement;
import org.adbcj.ResultEventHandler;
import org.adbcj.ResultSet;
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

    public MySqlPreparedStatement(AbstractMySqlConnection connection,StatementPreparedEOF statementInfo) {
        this.connection = connection;
        this.statementInfo = statementInfo;
    }

    @Override
    public DbFuture<ResultSet> executeQuery(final Object... params) {
        if(params.length!=statementInfo.getParams()){
            throw new IllegalArgumentException("Expect "+statementInfo.getParams()+" paramenters " +
                    "but got "+params.length+" parameters");
        }
        ResultEventHandler<DefaultResultSet> eventHandler = new AbstractDbSession.DefaultResultEventsHandler();
        DefaultResultSet resultSet = new DefaultResultSet(connection);


        return connection.enqueueTransactionalRequest(new ExpectResultRequest(connection,eventHandler, resultSet) {
            @Override
            public void execute() throws Exception {
                PreparedStatementRequest request = new PreparedStatementRequest(statementInfo.getHandlerId(), params);
                connection.write(request);
            }

            @Override
            public String toString() {
                return "Prepared statement execute";
            }
        });
    }
}
