package org.adbcj.connectionpool;

import org.adbcj.DbFuture;
import org.adbcj.DbListener;
import org.adbcj.PreparedStatement;

/**
 * @author roman.stoffel@gamlor.info
 */
public abstract class AbstractPooledPreparedStatement implements PooledResource {
    protected final PreparedStatement nativeQuery;
    protected final PooledConnection pooledConnection;
    private volatile DbFuture<Void> closeFuture = null;

    public AbstractPooledPreparedStatement(PreparedStatement nativeQuery, PooledConnection pooledConnection) {
        this.nativeQuery = nativeQuery;
        this.pooledConnection = pooledConnection;
    }

    public boolean isClosed() {
        return closeFuture!=null;
    }

    public DbFuture<Void> close() {
        synchronized (this){
            if(closeFuture!=null){
                return closeFuture;
            }
            closeFuture = pooledConnection.monitor(nativeQuery.close());
            closeFuture.addListener(new DbListener<Void>() {
                @Override
                public void onCompletion(DbFuture<Void> future) {
                    pooledConnection.removeResource(AbstractPooledPreparedStatement.this);
                }
            });
            return closeFuture;
        }
    }
}
