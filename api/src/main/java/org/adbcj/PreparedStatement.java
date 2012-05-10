package org.adbcj;

public interface PreparedStatement {


    boolean isClosed();

    DbFuture<Void> close();
}
