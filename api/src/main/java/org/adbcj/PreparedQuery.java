package org.adbcj;

public interface PreparedQuery extends PreparedStatement {

    DbFuture<ResultSet> execute(Object... params);

    <T> DbFuture<T> executeWithCallback(
            ResultHandler<T> eventHandler,
            T accumulator,Object... params);
}
