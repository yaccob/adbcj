package org.adbcj;

public interface PreparedQuery extends PreparedStatement {

    DbSessionFuture<ResultSet> execute(Object... params);

    <T> DbSessionFuture<T> executeWithCallback(
            ResultHandler<T> eventHandler,
            T accumulator,Object... params);
}
