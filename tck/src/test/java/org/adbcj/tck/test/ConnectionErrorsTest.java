package org.adbcj.tck.test;

import junit.framework.Assert;
import org.adbcj.Connection;
import org.adbcj.ConnectionManager;
import org.adbcj.ConnectionManagerProvider;
import org.adbcj.DbException;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author roman.stoffel@gamlor.info
 * @since 22.03.12
 */
public class ConnectionErrorsTest {
    @Parameters({"url", "user", "password"})
    @Test
    public void expectTimeoutWhenDatabaseNotAvailable(String url) throws Exception {
        assumeIsOnLocalhost(url);
        String unconnectableUrl = unconnectableURL(url);
        ConnectionManager connectionManager = ConnectionManagerProvider.createConnectionManager(unconnectableUrl, "root", "");
        try{
            Connection connection = connectionManager.connect().get();
            Assert.fail("should not be able to connect, but got" + connection);
        }catch (DbException e){
            // expected
        }
    }


    @Parameters({"url", "user", "password"})
    @Test
    public void expectErrorWithWrongSchema(String url, String user, String pwd) throws Exception {
        assumeIsOnLocalhost(url);
        String unconnectableUrl = wrongSchema(url);
        ConnectionManager connectionManager = ConnectionManagerProvider.createConnectionManager(unconnectableUrl, user, pwd);
        try{
            Connection connection = connectionManager.connect().get();
            Assert.fail("should not be able to connect, but got" + connection);
        }catch (DbException e){
            // expected
        }
    }

    private void assumeIsOnLocalhost(String url) {
        if(!url.contains("localhost")){
            Assert.fail("This test assumes that the database is on localhost");
        }

    }

    private String unconnectableURL(String url) throws Exception {
        return url.replace("localhost","not.reachable.localhost");
    }

    private String wrongSchema(String url) throws Exception {
        if(url.contains("h2")){
            return url.replace("adbcjtck","invalidschema;IFEXISTS=TRUE");
        }
        return url.replace("adbcjtck","invalidschema");
    }

}

