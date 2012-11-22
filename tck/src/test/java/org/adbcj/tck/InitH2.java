package org.adbcj.tck;

import org.h2.tools.Server;

/**
 * @author roman.stoffel@gamlor.info
 */
public class InitH2 extends InitDatabase {
    private Server server;

    @Override
    protected void loadDriver() throws ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
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
    protected void beforeSetupScript() {
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
        server.stop();
    }
}
