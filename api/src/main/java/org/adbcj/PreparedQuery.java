package org.adbcj;

public interface PreparedQuery extends PreparedStatement {

	DbFuture<ResultSet> execute(Object... params);
}
