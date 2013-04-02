package org.adbcj.connectionpool;

import org.adbcj.*;

/**
 * @author roman.stoffel@gamlor.info
 */
class PooledPreparedQuery extends AbstractPooledPreparedStatement implements PreparedQuery {

    public PooledPreparedQuery(StmtItem nativeQuery, PooledConnection pooledConnection) {
        super(nativeQuery, pooledConnection);
    }

    @Override
    public DbFuture<ResultSet> execute(Object... params) {
        pooledConnection.checkClosed();
        return pooledConnection.monitor(nativeQuery().execute(params));
    }

    @Override
    public <T> DbFuture<T> executeWithCallback(ResultHandler<T> eventHandler, T accumulator, Object... params) {
        pooledConnection.checkClosed();
        return pooledConnection.monitor(nativeQuery().executeWithCallback(eventHandler, accumulator, params));
    }

    private PreparedQuery nativeQuery() {
        return (PreparedQuery) stmt;
    }

}
