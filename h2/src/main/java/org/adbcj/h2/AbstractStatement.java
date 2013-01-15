package org.adbcj.h2;

import org.adbcj.DbFuture;
import org.adbcj.PreparedStatement;
import org.adbcj.support.DefaultDbFuture;

/**
 * @author roman.stoffel@gamlor.info
 */
public class AbstractStatement implements PreparedStatement {
    protected final H2Connection connection;
    protected final int sessionId;
    protected final int paramsCount;
    private volatile DefaultDbFuture<Void> closeFuture =null;

    public AbstractStatement(H2Connection connection, int sessionId, int paramsCount) {
        this.paramsCount = paramsCount;
        this.connection = connection;
        this.sessionId = sessionId;
    }

    @Override
    public boolean isClosed() {
        return closeFuture!=null;
    }

    @Override
    public DbFuture<Void> close() {
        synchronized (connection.connectionLock()){
            if(null!=closeFuture){
                return closeFuture;
            } else{
                final Request request = connection.requestCreator().executeCloseStatement(sessionId);
                this.closeFuture = (DefaultDbFuture<Void>) request.getToComplete();
                connection.forceQueRequest(request);
                return closeFuture;
            }
        }
    }
}
