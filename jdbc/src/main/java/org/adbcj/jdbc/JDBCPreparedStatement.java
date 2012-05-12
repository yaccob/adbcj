package org.adbcj.jdbc;

import org.adbcj.*;
import org.adbcj.support.AbstractDbSession;
import org.adbcj.support.DefaultResultEventsHandler;
import org.adbcj.support.DefaultResultSet;

import java.sql.SQLException;
import java.util.Collections;

import static org.adbcj.jdbc.ResultSetCopier.fillResultSet;

/**
 * @author roman.stoffel@gamlor.info
 * @since 10.05.12
 */
abstract class JDBCPreparedStatement<T> implements PreparedStatement {
    protected final java.sql.PreparedStatement sqlStatement;
    private JdbcConnection connection;

    public JDBCPreparedStatement(JdbcConnection connection, java.sql.PreparedStatement sqlStatement) {
        this.connection = connection;
        this.sqlStatement = sqlStatement;
    }

    // Implements the execute interface method of the sub types
    public DbFuture execute(final Object... params) {
        final int parameterCount;
        try {
            parameterCount = sqlStatement.getParameterMetaData().getParameterCount();
        } catch (SQLException e) {
            throw new DbException("Expect that PreparedStatement.getParameterMetaData() works",e);
        }
        if(params.length!=parameterCount){
            throw new IllegalArgumentException("Wrong amount of arguments." +
                    "This statement expects "+parameterCount+" but received "+params.length+" arguments");
        }
        return connection.enqueueTransactionalRequest(new AbstractDbSession.Request<T>(connection) {
            @Override
            protected void execute() throws Exception {
                int index = 1;
                for (Object param : params) {
                    sqlStatement.setObject(index, param);
                    index++;
                }
                complete(executeStatement());
            }
        });
    }

    protected abstract T executeStatement() throws Exception;

    @Override
    public boolean isClosed() {
        try {
            return sqlStatement.isClosed();
        } catch (SQLException e) {
            throw new DbException(e);
        }
    }

    @Override
    public DbFuture<Void> close() {
        return connection.enqueueTransactionalRequest(new AbstractDbSession.Request<Void>(connection) {
            @Override
            protected void execute() throws Exception {
                sqlStatement.close();
                complete(null);
            }
        });
    }
}

class JDBCPreparedQuery extends JDBCPreparedStatement<ResultSet> implements PreparedQuery {

    public JDBCPreparedQuery(JdbcConnection connection, java.sql.PreparedStatement sqlStatement) {
        super(connection, sqlStatement);
    }

    @Override
    protected ResultSet executeStatement() throws Exception {
        java.sql.ResultSet nativeResult = null;
        try {
            ResultEventHandler<DefaultResultSet> eventHandler = new DefaultResultEventsHandler();
            DefaultResultSet resultSet = new DefaultResultSet();
            nativeResult = sqlStatement.executeQuery();

            fillResultSet(nativeResult, eventHandler, resultSet);

            return resultSet;
        } finally {
            if (nativeResult != null) {
                nativeResult.close();
            }
        }
    }
}

class JDBCPreparedUpdate extends JDBCPreparedStatement<Result> implements PreparedUpdate {

    public JDBCPreparedUpdate(JdbcConnection connection, java.sql.PreparedStatement sqlStatement) {
        super(connection, sqlStatement);
    }

    @Override
    protected Result executeStatement() throws Exception {
        int affectedRows = sqlStatement.executeUpdate();

        return new JDBCResult(affectedRows, Collections.<String>emptyList(), sqlStatement.getGeneratedKeys());
    }
}
