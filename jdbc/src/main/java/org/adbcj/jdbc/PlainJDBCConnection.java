package org.adbcj.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * @author roman.stoffel@gamlor.info
 * @since 26.04.12
 */
public final class PlainJDBCConnection implements JDBCConnectionProvider {

    private static final String USER = "user";
    private static final String PASSWORD = "password";

    private final String jdbcUrl;
    private final Properties properties;

    public PlainJDBCConnection(String jdbcUrl, String username,String password, Map<String,String> properties) {
        this.jdbcUrl = jdbcUrl;
        this.properties = new Properties();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            properties.put(entry.getKey(),entry.getValue());
        }

        this.properties.put(USER, username);
        this.properties.put(PASSWORD, password);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, properties);
    }
}
