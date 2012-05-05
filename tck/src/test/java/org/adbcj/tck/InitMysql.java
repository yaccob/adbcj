package org.adbcj.tck;

import org.adbcj.jdbc.PlainJDBCConnection;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class InitMysql {


    @Parameters({"jdbcUrl", "user", "password"})
    @BeforeTest
    public void prepareMySQL(String url, String user, String password) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        runSQLScript(url, user, password, "sql/setupMySQL.sql");
    }

    @Parameters({"jdbcUrl", "user", "password"})
    @AfterSuite
    public void cleanUp(String jdbcUrl, String user, String password) {
        runSQLScript(jdbcUrl, user, password, "sql/cleanUpMySQL.sql");
    }

    private void runSQLScript(String jdbcUrl, String user, String password, String script) {
        try {
            Connection connection = new PlainJDBCConnection(jdbcUrl, user, password, new Properties()).getConnection();
            try {
                for (String line : setupSQL(script)) {
                    Statement stmt = connection.createStatement();
                    stmt.execute(line);
                    stmt.close();

                }

                System.out.println("Setup done");
            } finally {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage(), e);
        }
    }


    private List<String> setupSQL(String resourceName) {
        InputStream sqlData = getClass().getClassLoader().getResourceAsStream(resourceName);
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(sqlData));
            StringBuilder wholeFile = new StringBuilder();
            String line = reader.readLine();
            while (null != line) {
                wholeFile.append(line);
                line = reader.readLine();
            }
            return Arrays.asList(wholeFile.toString().split(";"));
        } catch (IOException e) {
            throw new RuntimeException("Couldn't read resource " + resourceName);
        } finally {
            try {
                sqlData.close();
            } catch (IOException e) {
                throw new RuntimeException("Couldn't read resource " + resourceName);
            }
        }
    }

}
