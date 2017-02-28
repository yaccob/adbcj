package org.adbcj;

import org.adbcj.support.DbCompletableFuture;

import java.util.concurrent.CompletableFuture;

public interface PreparedStatement {

    boolean isClosed();

    default CompletableFuture<Void> close(){
        DbCompletableFuture<Void> future = new DbCompletableFuture<>();
        close(future);
        return future;
    }

    void close(DbCallback<Void> callback);
}
