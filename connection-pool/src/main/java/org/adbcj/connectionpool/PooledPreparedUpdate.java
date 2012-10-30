package org.adbcj.connectionpool;

import org.adbcj.DbSessionFuture;
import org.adbcj.PreparedUpdate;
import org.adbcj.Result;

/**
 * @author roman.stoffel@gamlor.info
 */
class PooledPreparedUpdate extends AbstractPooledPreparedStatement implements PreparedUpdate {
    public PooledPreparedUpdate(PreparedUpdate nativeQuery, PooledConnection pooledConnection) {
        super(nativeQuery,pooledConnection);
    }

    @Override
    public DbSessionFuture<Result> execute(Object... params) {
        pooledConnection.checkClosed();
        return pooledConnection.monitor(nativeQuery().execute(params));
    }

    private PreparedUpdate nativeQuery() {
        return (PreparedUpdate)nativeQuery;
    }
}
