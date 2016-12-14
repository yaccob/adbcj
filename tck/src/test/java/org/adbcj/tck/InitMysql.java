package org.adbcj.tck;

/**
 * @author roman.stoffel@gamlor.info
 */
public class InitMysql extends InitDatabase {


    @Override
    protected String setupScript() {
        return "sql/setupMySQL.sql";
    }

    @Override
    protected String tearDownScript() {
        return "sql/cleanUpMySQL.sql";
    }
}
