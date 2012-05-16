package org.adbcj;

public interface PreparedQuery extends PreparedStatement {

	DbFuture<ResultSet> execute(Object... params);

    <T> DbSessionFuture<T> executeWithCallback(
            ResultEventHandler<T> eventHandler,
            T accumulator,Object... params);
}
