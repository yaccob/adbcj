package org.adbcj.mysql.codec;

import org.adbcj.DbFuture;
import org.adbcj.PreparedStatement;
import org.adbcj.ResultSet;

/**
 * @author roman.stoffel@gamlor.info
 * @since 11.04.12
 */
public class MySqlPreparedStatement implements PreparedStatement {
    private final AbstractMySqlConnection connection;
    private final OkResponse.PreparedStatementOK statementInfo;

    public MySqlPreparedStatement(AbstractMySqlConnection connection,OkResponse.PreparedStatementOK statementInfo) {
        this.connection = connection;
        this.statementInfo = statementInfo;
    }

    @Override
    public DbFuture<ResultSet> executeQuery(Object... params) {
        if(params.length!=statementInfo.getParams()){
            throw new IllegalArgumentException("Expect "+statementInfo.getParams()+" paramenters " +
                    "but got "+params.length+" parameters");
        }

        new CommandRequest(Command.STATEMENT_EXECUTE);

        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
