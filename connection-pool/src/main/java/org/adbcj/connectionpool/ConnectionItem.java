package org.adbcj.connectionpool;

import org.adbcj.Connection;
import org.adbcj.PreparedStatement;

import java.util.concurrent.atomic.AtomicInteger;


final class ConnectionItem {
    private final Connection nativeConnection;
    private final StmtCache stmtCache;

    public ConnectionItem(Connection nativeConnection, int cacheSize) {
        this.nativeConnection = nativeConnection;

        if(cacheSize>0){
            this.stmtCache = new SimpleStmt(cacheSize);
        } else{
            this.stmtCache = new NullCache();
        }
    }

    public Connection connection() {
        return nativeConnection;
    }

    public StmtCache stmtCache() {
        return stmtCache;
    }
}

final class StmtItem {
    private final AtomicInteger open = new AtomicInteger(0);
    private final PreparedStatement stmt;

    StmtItem(PreparedStatement stmt) {
        this.stmt = stmt;
    }


    public PreparedStatement aquireSharedAccess() {
        open.incrementAndGet();
        return stmt;
    }

    public void close() {
        if(open.decrementAndGet()==0){
            stmt.close();
        }
    }
}
