package org.adbcj.connectionpool;

import org.adbcj.DbFuture;
import org.adbcj.PreparedStatement;
import org.adbcj.support.DefaultDbFuture;

/**
 * @author roman.stoffel@gamlor.info
 */
public abstract class AbstractPooledPreparedStatement implements PooledResource {
    private final StmtItem stmtItem;
    protected final PreparedStatement stmt;
    protected final PooledConnection pooledConnection;
    private volatile DbFuture<Void> closeFuture = null;

    public AbstractPooledPreparedStatement(StmtItem nativeQuery, PooledConnection pooledConnection) {
        this.stmtItem = nativeQuery;
        stmt = this.stmtItem.aquireSharedAccess();
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
            closeFuture = DefaultDbFuture.completed(null);
            stmtItem.close();
            pooledConnection.removeResource(this);
            return closeFuture;
        }
    }
}
