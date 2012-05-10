/*
 *   Copyright (c) 2007 Mike Heath.  All rights reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package org.adbcj.jdbc;

import org.adbcj.*;
import org.adbcj.support.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.adbcj.jdbc.ResultSetCopier.fillResultSet;

public class JdbcConnection extends AbstractDbSession implements Connection {

    private final Logger logger = LoggerFactory.getLogger(JdbcConnection.class);

    private final JdbcConnectionManager connectionManager;
    private final java.sql.Connection jdbcConnection;
    private final ExecutorService threadPool;

    public JdbcConnection(JdbcConnectionManager connectionManager,
                          java.sql.Connection jdbcConnection,
                          ExecutorService threadPool) {
        super(false);
        this.connectionManager = connectionManager;
        this.jdbcConnection = jdbcConnection;
        this.threadPool = threadPool;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public synchronized DbFuture<Void> close() throws DbException {
        if (!isClosed()) {
            Request<Void> closeRequest = new Request<Void>(this) {
                @Override
                protected void execute() throws Exception {
                    jdbcConnection.close();
                    complete(null);
                }

                @Override
                public synchronized boolean cancelRequest(boolean mayInterruptIfRunning) {
                    return false;
                }

                @Override
                public boolean isPipelinable() {
                    return false;
                }
            };
            return enqueueRequest(closeRequest);
        } else{
            return DefaultDbSessionFuture.completed(null);
        }
    }

    public boolean isClosed() {
        try {
            return jdbcConnection.isClosed();
        } catch (SQLException e) {
            throw new DbException(this, e);
        }
    }

    public <T> DbSessionFuture<T> executeQuery(final String sql, final ResultEventHandler<T> eventHandler, final T accumulator) {
        checkClosed();
        logger.trace("Scheduling query '{}'", sql);
        return enqueueTransactionalRequest(new CallableRequest<T>() {
            @Override
            protected T doCall() throws Exception {
                logger.debug("Executing query '{}'", sql);
                Statement jdbcStatement = jdbcConnection.createStatement();
                java.sql.ResultSet jdbcResultSet = null;
                try {
                    // Execute query
                    jdbcResultSet = jdbcStatement.executeQuery(sql);
                    fillResultSet(jdbcResultSet, eventHandler, accumulator);


                    return accumulator;
                } finally {
                    if (jdbcResultSet != null) {
                        jdbcResultSet.close();
                    }
                    if (jdbcStatement != null) {
                        jdbcStatement.close();
                    }
                }
            }
        });
    }

    public DbSessionFuture<Result> executeUpdate(final String sql) {
        checkClosed();
        return enqueueTransactionalRequest(new CallableRequest<Result>() {
            public Result doCall() throws Exception {
                Statement statement = jdbcConnection.createStatement();
                try {
                    statement.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                    List<String> warnings = new LinkedList<String>();
                    SQLWarning sqlWarnings = statement.getWarnings();
                    while (sqlWarnings != null) {
                        warnings.add(sqlWarnings.getLocalizedMessage());
                        sqlWarnings = sqlWarnings.getNextWarning();
                    }
                    return new JDBCResult((long) statement.getUpdateCount(),
                            warnings,statement.getGeneratedKeys());
                } finally {
                    statement.close();
                }
            }
        });
    }

    public DbSessionFuture<PreparedQuery> prepareQuery(final String sql) {
        checkClosed();
        return enqueueTransactionalRequest(new CallableRequest<PreparedQuery>() {
            @Override
            protected PreparedQuery doCall() throws Exception {
                return new JDBCPreparedStatement(jdbcConnection.prepareStatement(sql));
            }
        });

    }

    @Override
    public DbSessionFuture<PreparedUpdate> prepareUpdate(String sql) {
        throw new UnsupportedOperationException("prepareUpdate is not supported yet");
    }

    @Override
    protected <E> void invokeExecuteWithCatch(final Request<E> request) {
        threadPool.submit(new Runnable() {
            @Override
            public void run() {
                JdbcConnection.super.invokeExecuteWithCatch(request);
            }
        });
    }

    /*
    *
    * End of API methods
    *
    */

    // *********** Transaction method implementations **************************

    @Override
    protected void sendBegin() throws SQLException {
        logger.trace("Sending begin");
        jdbcConnection.setAutoCommit(false);
    }

    @Override
    protected Request<Void> createBeginRequest(Transaction transaction) {
        logger.trace("Creating begin request");
        return new CallableRequestWrapper(super.createBeginRequest(transaction));
    }

    @Override
    protected void sendCommit() throws SQLException {
        logger.trace("Sending commit");
        jdbcConnection.commit();
    }

    @Override
    protected Request<Void> createCommitRequest(Transaction transaction) {
        logger.trace("Creating commit request");
        return new CallableRequestWrapper(super.createCommitRequest(transaction));
    }

    @Override
    protected void sendRollback() throws SQLException {
        logger.trace("Sending rollback");
        jdbcConnection.rollback();
    }

    @Override
    protected Request<Void> createRollbackRequest() {
        logger.trace("Creating rollback request");
        return new CallableRequestWrapper(super.createRollbackRequest());
    }

    // *********** JDBC Specific method implementations ************************

    @Override
    protected void checkClosed() {
        if (isClosed()) {
            throw new DbSessionClosedException(this, "Connection is closed");
        }
    }



    private abstract class CallableRequest<E> extends Request<E> implements Callable<E> {
        private Future<E> future = null;

        protected CallableRequest() {
            super(JdbcConnection.this);
        }

        @Override
        public boolean cancelRequest(boolean mayInterruptIfRunning) {
            if (future == null) {
                return true;
            }
            return future.cancel(mayInterruptIfRunning);
        }

        @Override
        final public void execute() {
            logger.trace("In CallableRequest.execute() processing request {}", this);
            this.future = connectionManager.getExecutorService().submit(this);
        }

        final public E call() throws Exception {
            if (isCancelled()) {
                return null;
            }
            try {
                E value = doCall();
                complete(value);
                return value;
            } catch (Exception e) {
                error(DbException.wrap(JdbcConnection.this, e));
                if (jdbcConnection.isClosed()) {
                    connectionManager.removeConnection(JdbcConnection.this);
                }
                throw e;
            }
        }

        protected abstract E doCall() throws Exception;

        @Override
        public boolean isPipelinable() {
            return false;
        }

    }

    private class CallableRequestWrapper extends CallableRequest<Void> {

        private final Request<Void> request;

        public CallableRequestWrapper(Request<Void> request) {
            this.request = request;
        }

        @Override
        public synchronized boolean cancelRequest(boolean mayInterruptIfRunning) {
            if (super.cancel(mayInterruptIfRunning)) {
                return request.cancel(mayInterruptIfRunning);
            }
            return false;
        }

        @Override
        protected Void doCall() throws Exception {
            request.invokeExecute();
            return null;
        }

        @Override
        public boolean canRemove() {
            return request.canRemove();
        }

        @Override
        public boolean isPipelinable() {
            return request.isPipelinable();
        }
    }

    private class JDBCPreparedStatement implements PreparedQuery{
        private final java.sql.PreparedStatement sqlStatement;

        public JDBCPreparedStatement(java.sql.PreparedStatement sqlStatement) {
            this.sqlStatement = sqlStatement;
        }

        @Override
        public DbFuture<ResultSet> execute(final Object... params) {
            return enqueueTransactionalRequest(new Request<ResultSet>(JdbcConnection.this) {
                @Override
                protected void execute() throws Exception {
                    int index = 1;
                    for (Object param : params) {
                        sqlStatement.setObject(index, param);
                        index++;
                    }
                    java.sql.ResultSet nativeResult = null;
                    try {
                        ResultEventHandler<DefaultResultSet> eventHandler = new DefaultResultEventsHandler();
                        DefaultResultSet resultSet = new DefaultResultSet();
                        nativeResult = sqlStatement.executeQuery();

                        fillResultSet(nativeResult, eventHandler, resultSet);

                        complete(resultSet);
                    } finally {
                        if (nativeResult != null) {
                            nativeResult.close();
                        }
                    }
                }
            });
        }


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
            return enqueueTransactionalRequest(new Request<Void>(JdbcConnection.this) {
                @Override
                protected void execute() throws Exception {
                    sqlStatement.close();
                    complete(null);
                }
            });
        }
    }

}