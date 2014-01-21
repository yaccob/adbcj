package org.adbcj.connectionpool;

import org.adbcj.*;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.FutureUtils;
import org.adbcj.support.OneArgFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class PooledConnection implements Connection, PooledResource {
    private static final Logger logger = LoggerFactory.getLogger(PooledConnection.class);
    private final ConnectionItem connectionItem;
    private final UsersConnectionPool pooledConnectionManager;
    private volatile DefaultDbFuture<Void> closingFuture;
    private final Map<DbFuture,DefaultDbFuture> runningOperations = new HashMap<DbFuture, DefaultDbFuture>();
    private final Set<AbstractPooledPreparedStatement> openStatements = new HashSet<AbstractPooledPreparedStatement>();
    private final Object collectionsLock = new Object();
    private volatile boolean mayBeCorrupted = false;
    private final DbListener operationsListener = new DbListener() {
        @Override
        public void onCompletion(DbFuture future) {
            synchronized (collectionsLock) {
                runningOperations.remove(future);
                if (isClosed()) {
                    mayFinallyCloseConnection();
                }
                if(future.getState()==FutureState.FAILURE){
                    logger.warn("Operation failed. Will close this connection and not return to the pool",future.getException());
                    PooledConnection.this.mayBeCorrupted = true;
                }
            }
        }
    };

    public PooledConnection(ConnectionItem connectionItem, UsersConnectionPool pooledConnectionManager) {
        this.connectionItem = connectionItem;
        this.pooledConnectionManager = pooledConnectionManager;
    }

    @Override
    public ConnectionManager getConnectionManager() {
        return pooledConnectionManager;
    }

    @Override
    public void beginTransaction() {
        checkClosed();
        nativeConnection().beginTransaction();
    }

    @Override
    public DbFuture<Void> commit() {
        checkClosed();
        return monitor(nativeConnection().commit());
    }

    @Override
    public DbFuture<Void> rollback() {
        checkClosed();
        return monitor(nativeConnection().rollback());
    }

    @Override
    public boolean isInTransaction() {
        checkClosed();
        return nativeConnection().isInTransaction();
    }

    @Override
    public DbFuture<ResultSet> executeQuery(String sql) {
        checkClosed();
        return monitor(nativeConnection().executeQuery(sql));
    }

    @Override
    public <T> DbFuture<T> executeQuery(String sql, ResultHandler<T> eventHandler, T accumulator) {
        checkClosed();
        return monitor(nativeConnection().executeQuery(sql, eventHandler, accumulator));
    }

    @Override
    public DbFuture<Result> executeUpdate(String sql) {
        checkClosed();
        return monitor(nativeConnection().executeUpdate(sql));
    }

    @Override
    public DbFuture<PreparedQuery> prepareQuery(final String sql) {
        checkClosed();
        final StmtItem stmtItem = connectionItem.stmtCache().get(sql);
        if(stmtItem!=null){
            return DefaultDbFuture.<PreparedQuery>completed(buildAndAddPooledQuery(stmtItem));
        } else{
            return monitor(nativeConnection().prepareQuery(sql), new OneArgFunction<PreparedQuery, PreparedQuery>() {
                @Override
                public PreparedQuery apply(PreparedQuery arg) {
                    StmtItem item = connectionItem.stmtCache().put(sql,arg);
                    return buildAndAddPooledQuery(item);
                }
            });
        }
    }

    @Override
    public DbFuture<PreparedUpdate> prepareUpdate(final String sql) {
        checkClosed();
        final StmtItem stmtItem = connectionItem.stmtCache().get(sql);
        if(stmtItem!=null){
            return DefaultDbFuture.<PreparedUpdate>completed(buildAndAddPooledUpdate(stmtItem));
        } else{
            return monitor(nativeConnection().prepareUpdate(sql), new OneArgFunction<PreparedUpdate, PreparedUpdate>() {
                @Override
                public PreparedUpdate apply(PreparedUpdate arg) {
                    StmtItem item = connectionItem.stmtCache().put(sql,arg);
                    return buildAndAddPooledUpdate(item);
                }
            });
        }
    }

    @Override
    public DbFuture<Void> close() throws DbException {
        return close(CloseMode.CLOSE_GRACEFULLY);
    }

    @Override
    public DbFuture<Void> close(CloseMode closeMode) throws DbException {
        synchronized (collectionsLock) {
            if (isClosed()) {
                return closingFuture;
            }
            closingFuture = new DefaultDbFuture<Void>(pooledConnectionManager.stackTracingOptions());
            if (closeMode == CloseMode.CANCEL_PENDING_OPERATIONS) {
                ArrayList<Map.Entry<DbFuture,DefaultDbFuture>> iterationCopy
                        = new ArrayList<Map.Entry<DbFuture,DefaultDbFuture>>(runningOperations.entrySet());
                for (Map.Entry<DbFuture,DefaultDbFuture> runningOperation : iterationCopy) {
                    runningOperation.getValue().trySetException(new DbSessionClosedException());
                    runningOperation.getKey().cancel(true);
                }
            }
        }
        closeStatements();
        mayFinallyCloseConnection();
        return closingFuture;
    }

    @Override
    public boolean isClosed() throws DbException {
        return closingFuture != null;
    }

    @Override
    public boolean isOpen() throws DbException {
        return nativeConnection().isOpen();
    }
    private Connection nativeConnection(){
        return connectionItem.connection();
    }

    private PooledPreparedUpdate buildAndAddPooledUpdate(StmtItem stmtItem) {
        PooledPreparedUpdate pooled = new PooledPreparedUpdate(stmtItem, PooledConnection.this);
        addStatement(pooled);
        return pooled;
    }
    private PooledPreparedQuery buildAndAddPooledQuery(StmtItem stmtItem) {
        PooledPreparedQuery pooled = new PooledPreparedQuery(stmtItem, PooledConnection.this);
        addStatement(pooled);
        return pooled;
    }

    private void addStatement(AbstractPooledPreparedStatement statement) {
        synchronized (collectionsLock){
            openStatements.add(statement);
        }
    }
    private void closeStatements(){
        ArrayList<AbstractPooledPreparedStatement> stmts;
        synchronized (collectionsLock){
            stmts= new ArrayList<AbstractPooledPreparedStatement>(openStatements);
            openStatements.clear();
        }
        for (AbstractPooledPreparedStatement openStatement : stmts) {
            openStatement.close();
        }

    }

    private void mayFinallyCloseConnection() {
        boolean noOperationRunning;
        synchronized (collectionsLock){
            noOperationRunning = runningOperations.isEmpty();
        }
        if(noOperationRunning){
            finallyClose();
        }
    }

    <T> DbFuture<T> monitor(DbFuture<T> futureToMonitor) {
        addMonitoring(futureToMonitor, (DefaultDbFuture) futureToMonitor);
        return futureToMonitor;
    }
    <TArgument,TResult> DbFuture<TResult> monitor(DbFuture<TArgument> futureToMonitor,
                                                  OneArgFunction<TArgument,TResult> transform) {
        final DefaultDbFuture<TResult> newFuture = FutureUtils.map(futureToMonitor, transform);
        addMonitoring(futureToMonitor, newFuture);
        return newFuture;
    }

    private <TArgument, TResult> void addMonitoring(DbFuture<TArgument> futureToMonitor, DefaultDbFuture<TResult> newFuture) {
        synchronized (collectionsLock){
            runningOperations.put(futureToMonitor, newFuture);
        }
        futureToMonitor.addListener(operationsListener);
    }


    Connection getNativeConnection() {
        return connectionItem.connection();
    }
    ConnectionItem connectionItem() {
        return connectionItem;
    }

    public boolean isMayBeCorrupted() {
        return mayBeCorrupted;
    }

    void checkClosed() {
        if (isClosed()) {
            throw new DbSessionClosedException("This connection is already closed");
        }
    }

    private void finallyClose() {
        if (closingFuture.trySetResult(null)) {
            pooledConnectionManager.returnConnection(this);
        }
    }

    void removeResource(AbstractPooledPreparedStatement dbListener) {
        synchronized (collectionsLock){
            openStatements.remove(dbListener);
        }
    }
}
