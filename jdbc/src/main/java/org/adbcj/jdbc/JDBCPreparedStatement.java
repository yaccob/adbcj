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
    protected final JdbcConnection connection;
    private final int paramenterCount;

    public JDBCPreparedStatement(JdbcConnection connection, java.sql.PreparedStatement sqlStatement) {
        this.connection = connection;
        this.sqlStatement = sqlStatement;
        synchronized (this.connection.lock()) {
            try {
                paramenterCount = sqlStatement.getParameterMetaData().getParameterCount();
            } catch (SQLException e) {
                throw new DbException("Expect that PreparedStatement.getParameterMetaData() works", e);
            }
        }
    }

    // Implements the execute interface method of the sub types
    @SuppressWarnings("UnusedDeclaration")
    public DbSessionFuture execute(final Object... params) {
        connection.checkClosed();
        return executeWithCompletion(new CompletionProducerFunction() {
            public T complete() throws Exception {
                return executeStatement();
            }
        }, params);
    }

    protected <TResult> DbSessionFuture executeWithCompletion(final CompletionProducerFunction<TResult> createCompletionAction, final Object[] params) {
        validateParameters(params);
        return connection.enqueueTransactionalRequest(new AbstractDbSession.Request<TResult>(connection) {
            @Override
            protected void execute() throws Exception {
                int index = 1;
                synchronized (connection.lock()) {
                    for (Object param : params) {
                        sqlStatement.setObject(index, param);
                        index++;
                    }
                    complete(createCompletionAction.complete());
                }
            }
        });
    }

    protected void validateParameters(Object[] params) {
        if (params.length != paramenterCount) {
            throw new IllegalArgumentException("Wrong amount of arguments." +
                    "This statement expects " + paramenterCount + " but received " + params.length + " arguments");
        }
    }

    protected abstract T executeStatement() throws Exception;

    @Override
    public boolean isClosed() {
        synchronized (connection.lock()) {
            try {
                return sqlStatement.isClosed();
            } catch (SQLException e) {
                throw new DbException(e);
            }
        }
    }

    @Override
    public DbFuture<Void> close() {
        return connection.enqueueTransactionalRequest(new AbstractDbSession.Request<Void>(connection) {
            @Override
            protected void execute() throws Exception {
                synchronized (connection.lock()) {
                    sqlStatement.close();
                }
                complete(null);
            }
        });
    }

    static abstract class CompletionProducerFunction<T> {
        public abstract T complete() throws Exception;
    }
}

class JDBCPreparedQuery extends JDBCPreparedStatement<ResultSet> implements PreparedQuery {

    public JDBCPreparedQuery(JdbcConnection connection, java.sql.PreparedStatement sqlStatement) {
        super(connection, sqlStatement);
    }

    @Override
    public <T> DbSessionFuture<T> executeWithCallback(final ResultHandler<T> eventHandler, final T accumulator, Object... params) {
        return executeWithCompletion(new CompletionProducerFunction<T>() {
            @Override
            public T complete() throws Exception {
                return executeStatement(eventHandler, accumulator);
            }
        }, params);
    }

    @Override
    protected ResultSet executeStatement() throws Exception {
        return executeStatement(new DefaultResultEventsHandler(), new DefaultResultSet());
    }

    private <T> T executeStatement(ResultHandler<T> eventHandler, T accumulator) throws Exception {
        java.sql.ResultSet nativeResult = null;
        try {
            synchronized (connection.lock()){
                nativeResult = sqlStatement.executeQuery();

                fillResultSet(nativeResult, eventHandler, accumulator);

            }

            return accumulator;
        } catch (Exception e) {
            eventHandler.exception(e, accumulator);
            throw e;
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
        synchronized (this.connection.lock()) {
            int affectedRows = sqlStatement.executeUpdate();

            return new JDBCResult(affectedRows, Collections.<String>emptyList(), sqlStatement.getGeneratedKeys());
        }
    }
}
