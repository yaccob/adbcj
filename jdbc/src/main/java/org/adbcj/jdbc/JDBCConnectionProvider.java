package org.adbcj.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author roman.stoffel@gamlor.info
 * @since 26.04.12
 */
public interface JDBCConnectionProvider {
    Connection getConnection() throws SQLException;
    Connection getConnection(String user,String password) throws SQLException;

}
