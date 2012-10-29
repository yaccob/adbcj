package org.adbcj.connectionpool;

import org.adbcj.DbFuture;
import org.adbcj.PreparedStatement;

/**
 * @author roman.stoffel@gamlor.info
 */
public class AbstractPooledPreparedStatement implements PooledResource {
    protected final PreparedStatement nativeQuery;
    protected final PooledConnection pooledConnection;

    public AbstractPooledPreparedStatement(PreparedStatement nativeQuery, PooledConnection pooledConnection) {
        this.nativeQuery = nativeQuery;
        this.pooledConnection = pooledConnection;
    }

    public boolean isClosed() {
        return nativeQuery.isClosed();
    }

    public DbFuture<Void> close() {
        return pooledConnection.monitor(nativeQuery.close());
    }
}
