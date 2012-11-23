package org.adbcj.tck;

import org.adbcj.jdbc.PlainJDBCConnection;
import org.h2.tools.Server;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * @author roman.stoffel@gamlor.info
 */
public class InitH2 extends InitDatabase {
    private Server server;

    @Override
    protected void loadDriver() throws ClassNotFoundException {
    }

    @Override
    protected String setupScript() {
        return "sql/setupH2.sql";
    }

    @Override
    protected String tearDownScript() {
        return "sql/cleanUpH2.sql";
    }

    @Override
    protected void beforeSetupScript(String jdbcUrl, String user, String password) {

        try {
            Connection connection  = new PlainJDBCConnection(jdbcUrl,
                        user,
                        password,
                        new HashMap<String, String>()).getConnection();
            if (null != connection) {
                connection.close();
                return;
            }
        } catch (SQLException e) {
            // expected, server not running
        }
        try {
//            this.server = Server.createPgServer("-pgAllowOthers",
//                    "-pgDaemon",
//                    "-pgPort", "14242",
//                    "-baseDir", "./h2testdb");
            this.server = Server.createTcpServer("-tcpAllowOthers",
                    "-tcpDaemon",
                    "-tcpPort", "14242",
                    "-baseDir", "./h2testdb");
            server.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    protected void afterCleanupScript() {
    }
}
